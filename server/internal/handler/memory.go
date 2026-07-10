package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/metrics"
	"github.com/qiffang/mnemos/server/internal/runtimeusage"
	"github.com/qiffang/mnemos/server/internal/service"
)

var (
	// Keep the application timeout below the client's 10m request timeout
	// so slow sync ingest returns a structured JSON 504 instead of a socket-level abort.
	syncIngestTimeout = 9 * time.Minute
)

type createMemoryRequest struct {
	Content            string                  `json:"content,omitempty"`
	MemoryType         string                  `json:"memory_type,omitempty"`
	AgentID            string                  `json:"agent_id,omitempty"`
	AppID              string                  `json:"appId,omitempty"`
	AppIDLegacy        string                  `json:"app_id,omitempty"`
	Tags               []string                `json:"tags,omitempty"`
	Metadata           json.RawMessage         `json:"metadata,omitempty"`
	Messages           []service.IngestMessage `json:"messages,omitempty"`
	SessionID          string                  `json:"session_id,omitempty"`
	Mode               service.IngestMode      `json:"mode,omitempty"`
	Sync               bool                    `json:"sync,omitempty"`
	DisableSessionSave bool                    `json:"disableSessionSave,omitempty"`
	// Keys is the agent-side retrieval-key list. When non-empty, the
	// K=>V store path skips the server-side extractkeys.Extract LLM call
	// and uses these keys directly (after server-side validation).
	// Empty/absent keeps the current server-extract behavior.
	Keys []service.RetrievalKey `json:"keys,omitempty"`
}

// agentKeysCreateResponse is the sync POST response body when the
// caller supplied agent-side retrieval_keys. The agent-side
// Mem9MemoryStore tool parses this and surfaces accepted/rejected
// counts back to the agent so subsequent stores can self-correct.
// When no agent keys were supplied the handler returns the simpler
// `{"status":"ok"}` shape for backward compatibility.
type agentKeysCreateResponse struct {
	Status       string                `json:"status"`
	MemoryID     string                `json:"memory_id,omitempty"`
	KeysInserted int                   `json:"keys_inserted"`
	KeysRejected []service.RejectedKey `json:"keys_rejected"`
}

func memID(m *domain.Memory) string {
	if m == nil {
		return ""
	}
	return m.ID
}

func isSyncIngestTimeout(ctx context.Context, err error) bool {
	return err != nil && errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func (s *Server) createMemory(w http.ResponseWriter, r *http.Request) {
	var req createMemoryRequest
	if err := decode(r, &req); err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	auth := authInfo(r)
	var chainAuth *domain.AuthInfo
	var writeChainSource *domain.ChainSource
	if auth.IsChain() {
		var err error
		chainAuth = auth
		if len(auth.Chain.Nodes) > 0 {
			writeChainSource = chainSource(auth, auth.Chain.Nodes[0])
		}
		auth, err = s.firstChainNodeAuth(auth)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
	}
	svc := s.resolveServices(auth)

	agentID := req.AgentID
	if agentID == "" {
		agentID = auth.AgentName
	}
	appID, err := appIDFromCreateRequest(req)
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	hasMessages := len(req.Messages) > 0
	hasContent := strings.TrimSpace(req.Content) != ""

	if hasMessages && hasContent {
		s.handleError(r.Context(), w, &domain.ValidationError{Field: "body", Message: "provide either content or messages, not both"})
		return
	}

	if hasMessages && strings.TrimSpace(req.MemoryType) != "" {
		s.handleError(r.Context(), w, &domain.ValidationError{Field: "memory_type", Message: "memory_type is only allowed with content, not messages"})
		return
	}

	if hasMessages {
		messages := append([]service.IngestMessage(nil), req.Messages...)
		ingestReq := service.IngestRequest{
			Messages:           messages,
			SessionID:          req.SessionID,
			AgentID:            agentID,
			AppID:              appID,
			Mode:               req.Mode,
			DisableSessionSave: s.disableSessionSave || req.DisableSessionSave,
			Metadata:           append(json.RawMessage(nil), req.Metadata...),
			// Forward agent-supplied retrieval keys so messages-shape
			// callers (the agent's Mem9MemoryStore tool is the primary
			// one) actually exercise the agent-K path on the server.
			// Without this, req.Keys was unmarshalled from the body but
			// dropped here, and every messages-shape store quietly fell
			// back to server-side extractkeys.Extract. Caught in
			// a code review.
			Keys: req.Keys,
		}

		if req.Sync {
			syncCtx, cancel := context.WithTimeout(r.Context(), syncIngestTimeout)
			defer cancel()

			var lease *runtimeusage.OperationLease
			finalized := false
			if s.runtimeUsageEnabled() {
				var err error
				lease, err = s.runtimeUsage.BeforeMemoryCreate(syncCtx, subjectFromAuth(auth), 1)
				if err != nil {
					s.handleRuntimeUsageError(w, err)
					return
				}
				defer func() {
					if !finalized {
						s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, context.Canceled)
					}
				}()
			}

			result, err := s.ingestMessages(syncCtx, auth, svc, ingestReq, chainAuth)
			if err != nil {
				if s.runtimeUsageEnabled() {
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					finalized = true
				}
				if isSyncIngestTimeout(syncCtx, err) {
					s.logger.Warn("sync ingest timed out", "session", ingestReq.SessionID, "timeout", syncIngestTimeout)
					respondError(w, http.StatusGatewayTimeout, fmt.Sprintf("sync ingest timed out after %s", syncIngestTimeout))
					return
				}
				s.handleError(syncCtx, w, err)
				return
			}
			if result != nil && result.Status == "failed" {
				if s.runtimeUsageEnabled() {
					err := errors.New("ingest reconciliation failed")
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					finalized = true
				}
				respondError(w, http.StatusInternalServerError, "ingest reconciliation failed")
				return
			}
			var written int64
			if result != nil {
				written = int64(result.MemoriesChanged)
			}
			if s.runtimeUsageEnabled() {
				var ids []string
				if result != nil {
					ids = result.InsightIDs
				}
				if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
					return s.runtimeUsage.AfterMemoryCreateSuccess(ctx, lease, runtimeusage.MemoryCreateResult{
						MemoryIDs:       ids,
						AgentName:       auth.AgentName,
						ObjectsAffected: written,
					})
				}); err != nil {
					s.logger.Error("runtime usage sync ingest finalization failed",
						"operation_id", lease.OperationID,
						"tenant_id", auth.TenantID,
						"cluster_id", auth.ClusterID,
						"err", err)
					finalized = true
					s.handleRuntimeUsageError(w, err)
					return
				}
				finalized = true
			}
			s.recordIngestMetering(auth, svc)
			s.enqueueMemoryAddedIDWebhooks(syncCtx, auth, svc, writeChainSource, chainAuth, result)
			go s.afterSuccessfulWrite(auth, svc, written)
			// When the caller supplied agent-side retrieval keys, return
			// the full agentKeysCreateResponse shape so the agent's
			// Mem9MemoryStore tool can surface rejection reasons back to
			// the agent for self-correction. Legacy callers that didn't
			// send keys keep the simpler {"status":"ok"} shape.
			if len(req.Keys) > 0 {
				resp := agentKeysCreateResponse{
					Status:       "ok",
					KeysRejected: []service.RejectedKey{},
				}
				if result != nil {
					if len(result.InsightIDs) > 0 {
						resp.MemoryID = result.InsightIDs[0]
					}
					resp.KeysInserted = result.KeysInserted
					if len(result.KeysRejected) > 0 {
						resp.KeysRejected = result.KeysRejected
					}
				}
				respond(w, http.StatusOK, resp)
			} else {
				respond(w, http.StatusOK, map[string]string{"status": "ok"})
			}
		} else {
			var lease *runtimeusage.OperationLease
			if s.runtimeUsageEnabled() {
				var err error
				lease, err = s.runtimeUsage.BeforeMemoryCreate(r.Context(), subjectFromAuth(auth), 1)
				if err != nil {
					s.handleRuntimeUsageError(w, err)
					return
				}
			}
			go func(lease *runtimeusage.OperationLease) {
				result, err := s.ingestMessages(context.Background(), auth, svc, ingestReq, chainAuth)
				if err != nil {
					if s.runtimeUsageEnabled() {
						s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					}
					slog.Error("async ingest failed", "session", ingestReq.SessionID, "err", err)
					return
				}
				if result != nil && result.Status == "failed" {
					if s.runtimeUsageEnabled() {
						s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, errors.New("ingest reconciliation failed"))
					}
					slog.Error("async ingest reconcile failed", "session", ingestReq.SessionID)
					return
				}
				var written int64
				if result != nil {
					written = int64(result.MemoriesChanged)
				}
				if s.runtimeUsageEnabled() {
					var ids []string
					if result != nil {
						ids = result.InsightIDs
					}
					if err := s.runtimeUsage.AfterMemoryCreateSuccess(context.Background(), lease, runtimeusage.MemoryCreateResult{
						MemoryIDs:       ids,
						AgentName:       auth.AgentName,
						ObjectsAffected: written,
					}); err != nil {
						s.logger.Error("runtime usage async ingest finalization failed",
							"operation_id", lease.OperationID,
							"tenant_id", auth.TenantID,
							"cluster_id", auth.ClusterID,
							"err", err)
						return
					}
				}
				s.enqueueMemoryAddedIDWebhooks(context.Background(), auth, svc, writeChainSource, chainAuth, result)
				s.afterSuccessfulIngest(auth, svc, written)
			}(lease)
			respond(w, http.StatusAccepted, map[string]string{"status": "accepted"})
		}
		return
	}

	if !hasContent {
		s.handleError(r.Context(), w, &domain.ValidationError{Field: "content", Message: "content or messages required"})
		return
	}
	if req.Mode != "" {
		s.handleError(r.Context(), w, &domain.ValidationError{Field: "body", Message: "content mode does not accept mode"})
		return
	}

	tags := append([]string(nil), req.Tags...)
	metadata := append(json.RawMessage(nil), req.Metadata...)
	content := req.Content
	explicitMemoryType := strings.TrimSpace(req.MemoryType)

	if explicitMemoryType != "" {
		if explicitMemoryType != string(domain.TypePinned) {
			s.handleError(r.Context(), w, &domain.ValidationError{
				Field:   "memory_type",
				Message: fmt.Sprintf("unsupported value %q; only %q is supported on the explicit content path", explicitMemoryType, domain.TypePinned),
			})
			return
		}

		var lease *runtimeusage.OperationLease
		finalized := false
		if s.runtimeUsageEnabled() {
			var err error
			lease, err = s.runtimeUsage.BeforeMemoryCreate(r.Context(), subjectFromAuth(auth), 1)
			if err != nil {
				s.handleRuntimeUsageError(w, err)
				return
			}
			defer func() {
				if !finalized {
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, context.Canceled)
				}
			}()
		}

		mem, written, err := svc.memory.CreatePinned(r.Context(), agentID, appID, content, tags, metadata)
		if err != nil {
			if s.runtimeUsageEnabled() {
				s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
				finalized = true
			}
			slog.Error("pinned memory create failed", "agent", agentID, "actor", auth.AgentName, "err", err)
			s.handleError(r.Context(), w, err)
			return
		}
		if mem != nil {
			mem.ChainSource = writeChainSource
		}
		if s.runtimeUsageEnabled() {
			memoryID := ""
			if mem != nil {
				memoryID = mem.ID
			}
			var ids []string
			if memoryID != "" {
				ids = []string{memoryID}
			}
			if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
				return s.runtimeUsage.AfterMemoryCreateSuccess(ctx, lease, runtimeusage.MemoryCreateResult{
					MemoryIDs:       ids,
					AgentName:       auth.AgentName,
					ObjectsAffected: int64(written),
				})
			}); err != nil {
				s.logger.Error("runtime usage memory create finalization failed",
					"operation_id", lease.OperationID,
					"tenant_id", auth.TenantID,
					"cluster_id", auth.ClusterID,
					"err", err)
				finalized = true
				s.handleRuntimeUsageError(w, err)
				return
			}
			finalized = true
		}
		s.enqueueMemoryAddedWebhook(r.Context(), auth, writeChainSource, chainAuth, mem)
		go s.afterSuccessfulWrite(auth, svc, int64(written))
		respond(w, http.StatusCreated, mem)
		return
	}

	if req.Sync {
		var lease *runtimeusage.OperationLease
		finalized := false
		if s.runtimeUsageEnabled() {
			var err error
			lease, err = s.runtimeUsage.BeforeMemoryCreate(r.Context(), subjectFromAuth(auth), 1)
			if err != nil {
				s.handleRuntimeUsageError(w, err)
				return
			}
			defer func() {
				if !finalized {
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, context.Canceled)
				}
			}()
		}
		routingTargets := chainRoutingTargets(chainAuth)
		if len(routingTargets) > 0 && svc.ingest.HasLLM() {
			result, err := s.createSmartContentWithRouting(r.Context(), chainAuth, svc, routingTargets, auth.AgentName, agentID, appID, req.SessionID, content, tags, metadata)
			if err != nil {
				if s.runtimeUsageEnabled() {
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					finalized = true
				}
				slog.Error("sync routed memory create failed", "agent", agentID, "actor", auth.AgentName, "err", err)
				s.handleError(r.Context(), w, err)
				return
			}
			if result != nil && result.Status == "failed" {
				if s.runtimeUsageEnabled() {
					err := errors.New("content reconciliation failed")
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					finalized = true
				}
				respondError(w, http.StatusInternalServerError, "content reconciliation failed")
				return
			}
			var written int64
			var ids []string
			if result != nil {
				written = int64(result.MemoriesChanged)
				ids = result.InsightIDs
			}
			if s.runtimeUsageEnabled() {
				if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
					return s.runtimeUsage.AfterMemoryCreateSuccess(ctx, lease, runtimeusage.MemoryCreateResult{
						MemoryIDs:       ids,
						AgentName:       auth.AgentName,
						ObjectsAffected: written,
					})
				}); err != nil {
					s.logger.Error("runtime usage sync routed memory create finalization failed",
						"operation_id", lease.OperationID,
						"tenant_id", auth.TenantID,
						"cluster_id", auth.ClusterID,
						"err", err)
					finalized = true
					s.handleRuntimeUsageError(w, err)
					return
				}
				finalized = true
			}
			s.enqueueMemoryAddedIDWebhooks(r.Context(), auth, svc, writeChainSource, chainAuth, result)
			go s.afterSuccessfulWrite(auth, svc, written)
			respond(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		// s.persistContentSession(r.Context(), auth, svc, req.SessionID, agentID, content, metadata)
		mem, written, keysInserted, keysRejected, err := svc.memory.CreateWithAgentKeys(
			r.Context(), agentID, appID, content, tags, metadata, req.Keys)
		if err != nil {
			if s.runtimeUsageEnabled() {
				s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
				finalized = true
			}
			slog.Error("sync memory create failed", "agent", agentID, "actor", auth.AgentName, "err", err)
			s.handleError(r.Context(), w, err)
			return
		}
		if s.runtimeUsageEnabled() {
			var ids []string
			if mem != nil && mem.ID != "" {
				ids = []string{mem.ID}
			}
			if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
				return s.runtimeUsage.AfterMemoryCreateSuccess(ctx, lease, runtimeusage.MemoryCreateResult{
					MemoryIDs:       ids,
					AgentName:       auth.AgentName,
					ObjectsAffected: int64(written),
				})
			}); err != nil {
				s.logger.Error("runtime usage sync memory create finalization failed",
					"operation_id", lease.OperationID,
					"tenant_id", auth.TenantID,
					"cluster_id", auth.ClusterID,
					"err", err)
				finalized = true
				s.handleRuntimeUsageError(w, err)
				return
			}
			finalized = true
		}
		s.enqueueMemoryAddedWebhook(r.Context(), auth, writeChainSource, chainAuth, mem)
		go s.afterSuccessfulWrite(auth, svc, int64(written))
		// Sync-response surfacing of agent-keys outcome: when the agent
		// supplied retrieval_keys, return per-key inserted/rejected counts so
		// the Mem9MemoryStore tool can show the agent which
		// keys were dropped and why. When no agent keys were supplied
		// we keep the legacy `{"status":"ok"}` shape to avoid
		// surprising old callers (the keysInserted>0 / keysRejected>0
		// conditions are unreachable when len(req.Keys)==0, so the
		// condition simplifies to a single len check).
		if len(req.Keys) > 0 {
			respond(w, http.StatusOK, agentKeysCreateResponse{
				Status:       "ok",
				MemoryID:     memID(mem),
				KeysInserted: keysInserted,
				KeysRejected: keysRejected,
			})
		} else {
			respond(w, http.StatusOK, map[string]string{"status": "ok"})
		}
	} else {
		var lease *runtimeusage.OperationLease
		if s.runtimeUsageEnabled() {
			var err error
			lease, err = s.runtimeUsage.BeforeMemoryCreate(r.Context(), subjectFromAuth(auth), 1)
			if err != nil {
				s.handleRuntimeUsageError(w, err)
				return
			}
		}
		routingTargets := chainRoutingTargets(chainAuth)
		go func(auth, chainAuth *domain.AuthInfo, lease *runtimeusage.OperationLease, agentName, actorAgentID, appID, sessionID, content string, tags []string, metadata json.RawMessage, routingTargets []service.RoutingTarget) {
			// s.persistContentSession(context.Background(), auth, svc, sessionID, actorAgentID, content, metadata)
			if len(routingTargets) > 0 && svc.ingest.HasLLM() {
				result, err := s.createSmartContentWithRouting(context.Background(), chainAuth, svc, routingTargets, agentName, actorAgentID, appID, sessionID, content, tags, metadata)
				if err != nil {
					if s.runtimeUsageEnabled() {
						s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
					}
					slog.Error("async routed memory create failed", "agent", actorAgentID, "actor", agentName, "err", err)
					return
				}
				if result != nil && result.Status == "failed" {
					if s.runtimeUsageEnabled() {
						s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, errors.New("content reconciliation failed"))
					}
					slog.Error("async routed memory reconcile failed", "agent", actorAgentID, "actor", agentName)
					return
				}
				var written int64
				var ids []string
				if result != nil {
					written = int64(result.MemoriesChanged)
					ids = result.InsightIDs
				}
				if s.runtimeUsageEnabled() {
					if err := s.runtimeUsage.AfterMemoryCreateSuccess(context.Background(), lease, runtimeusage.MemoryCreateResult{
						MemoryIDs:       ids,
						AgentName:       auth.AgentName,
						ObjectsAffected: written,
					}); err != nil {
						s.logger.Error("runtime usage async routed memory create finalization failed",
							"operation_id", lease.OperationID,
							"tenant_id", auth.TenantID,
							"cluster_id", auth.ClusterID,
							"err", err)
						return
					}
				}
				s.enqueueMemoryAddedIDWebhooks(context.Background(), auth, svc, writeChainSource, chainAuth, result)
				s.afterSuccessfulWrite(auth, svc, written)
				return
			}
			mem, written, err := svc.memory.Create(context.Background(), actorAgentID, appID, content, tags, metadata)
			if err != nil {
				if s.runtimeUsageEnabled() {
					s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
				}
				slog.Error("async memory create failed", "agent", actorAgentID, "actor", agentName, "err", err)
				return
			}
			if mem != nil {
				slog.Info("async memory create complete", "agent", actorAgentID, "actor", agentName, "memory_id", mem.ID)
			} else {
				slog.Info("async memory create complete", "agent", actorAgentID, "actor", agentName, "memory_id", "")
			}
			if s.runtimeUsageEnabled() {
				var ids []string
				if mem != nil && mem.ID != "" {
					ids = []string{mem.ID}
				}
				if err := s.runtimeUsage.AfterMemoryCreateSuccess(context.Background(), lease, runtimeusage.MemoryCreateResult{
					MemoryIDs:       ids,
					AgentName:       auth.AgentName,
					ObjectsAffected: int64(written),
				}); err != nil {
					s.logger.Error("runtime usage async memory create finalization failed",
						"operation_id", lease.OperationID,
						"tenant_id", auth.TenantID,
						"cluster_id", auth.ClusterID,
						"err", err)
					return
				}
			}
			s.enqueueMemoryAddedWebhook(context.Background(), auth, writeChainSource, chainAuth, mem)
			s.afterSuccessfulWrite(auth, svc, int64(written))
		}(auth, chainAuth, lease, auth.AgentName, agentID, appID, req.SessionID, content, tags, metadata, routingTargets)

		respond(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	}
}

// ingestMessages routes a messages-shape memory_store request through
// the K=>V store path (step 4.3 of the K=>V refactor).
//
// Pre-K=>V, this function ran a multi-phase pipeline:
//
//	session BulkCreate
//	  → ExtractPhase1 (LLM extracts facts from messages)
//	    → ReconcilePhase2 (LLM merges facts against existing memories)
//	      → (optional) reconcileRoutedChainFacts
//
// That pipeline wrote to the legacy `memories` table. The K=>V refactor
// replaces that write path; per a scope decision
// ("memory_store must work via K=>V; agent clients use the messages
// shape"), this function now concatenates the user-role messages'
// content and runs it through svc.memory.Create — which step 4 already
// switched to extractkeys.Extract + repo.UpsertMemoryValue +
// repo.InsertMemoryKeys.
//
// Behavior changes vs the pre-K=>V pipeline (documented):
//   - Always produces exactly 1 V per request (was: 0..N facts each
//     extracted into separate insights).
//   - LLM is used for K (retrieval-key) extraction only, not for fact
//     extraction from messages or for content reconciliation.
//   - Reconciliation/merge/insight-detection semantics are gone; the
//     content_hash dedup at the repo layer handles "same content seen
//     before"; multi-K alias handles "same fact, multiple phrasings".
//
// Preserved: session BulkCreate (raw conversation persistence) still
// runs best-effort, so /session-messages keeps working independent of
// the memory write path.
func (s *Server) ingestMessages(ctx context.Context, auth *domain.AuthInfo, svc resolvedSvc, req service.IngestRequest, chainAuth *domain.AuthInfo) (*service.IngestResult, error) {
	start := time.Now()
	var (
		bulkCreateDuration time.Duration
		storeDuration      time.Duration
		status             = "ok"
	)
	defer func() {
		s.logger.Info("messages ingest timings (k=>v)",
			"session", req.SessionID,
			"messages", len(req.Messages),
			"status", status,
			"bulk_create_ms", bulkCreateDuration.Milliseconds(),
			"store_ms", storeDuration.Milliseconds(),
			"total_ms", time.Since(start).Milliseconds(),
		)
	}()
	_ = chainAuth // chain-routed reconcile is not part of the K=>V path

	// Strip plugin-injected context (e.g. <relevant-memories>) before any storage or LLM path.
	req.Messages = service.StripInjectedContext(req.Messages)

	if !req.DisableSessionSave {
		// Session persistence is best-effort for both sync and async paths.
		bulkCreateStart := time.Now()
		if err := svc.session.BulkCreate(ctx, auth.AgentName, req); err != nil {
			slog.Error("session raw save failed",
				"cluster_id", auth.ClusterID, "session", req.SessionID, "err", err)
		}
		bulkCreateDuration = time.Since(bulkCreateStart)
	}

	// Concatenate user-role messages' content as the canonical V text.
	// Assistant-role messages are reply text and don't carry the fact.
	content := concatUserMessages(req.Messages)
	if strings.TrimSpace(content) == "" {
		status = "empty_user_content"
		return &service.IngestResult{Status: "complete"}, nil
	}

	storeStart := time.Now()
	mem, _, keysInserted, rejectedKeys, err := svc.memory.CreateWithAgentKeys(
		ctx, req.AgentID, req.AppID, content, nil, req.Metadata, req.Keys)
	storeDuration = time.Since(storeStart)
	if err != nil {
		status = "store_kv_error"
		slog.Error("k=>v store failed", "session", req.SessionID, "err", err)
		return nil, fmt.Errorf("k=>v store: %w", err)
	}
	if len(rejectedKeys) > 0 {
		slog.Info("agent-provided keys partially rejected",
			"session", req.SessionID,
			"rejected", len(rejectedKeys))
	}

	reconcileResult := &service.IngestResult{
		Status:          "complete",
		MemoriesChanged: 1,
		KeysInserted:    keysInserted,
		KeysRejected:    rejectedKeys,
	}
	if mem != nil {
		reconcileResult.InsightIDs = []string{mem.ID}
	}
	var reconcileErr error // K=>V doesn't run reconciliation; preserved for shape-compat below
	_ = reconcileErr

	// svc.memory.Create above is the entire write path now (K=>V replaces
	// the previous Extract/Reconcile/route pipeline). reconcileResult is
	// the IngestResult shape preserved for HTTP response compatibility.
	if reconcileResult.Status != "" {
		status = reconcileResult.Status
	}
	return reconcileResult, nil
}

// concatUserMessages flattens IngestMessages' user-role content into a
// single string used as the V's content by the K=>V store path. Assistant
// turns are skipped (they are reply text, not the fact to remember).
// System / tool messages are also skipped. Trims whitespace and joins
// with "\n\n" to keep multi-turn user inputs separable.
func concatUserMessages(messages []service.IngestMessage) string {
	if len(messages) == 0 {
		return ""
	}
	parts := make([]string, 0, len(messages))
	for _, m := range messages {
		if m.Role != "" && m.Role != "user" {
			continue
		}
		t := strings.TrimSpace(m.Content)
		if t == "" {
			continue
		}
		parts = append(parts, t)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n\n")
}

func (s *Server) createSmartContentWithRouting(ctx context.Context, chainAuth *domain.AuthInfo, svc resolvedSvc, routingTargets []service.RoutingTarget, agentName, agentID, appID, sessionID, content string, tags []string, metadata json.RawMessage) (*service.IngestResult, error) {
	facts, err := svc.ingest.ExtractContentWithRouting(ctx, content, routingTargets)
	if err != nil {
		return nil, err
	}
	if len(facts) == 0 {
		return &service.IngestResult{Status: "complete"}, nil
	}
	result, err := svc.ingest.ReconcilePhase2(ctx, agentName, agentID, appID, sessionID, facts)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = &service.IngestResult{Status: "complete"}
	}
	if result.Status == "failed" {
		return result, nil
	}

	patchWrites := 0
	if len(tags) > 0 || len(metadata) > 0 {
		for _, id := range result.InsightIDs {
			if _, err := svc.memory.Update(ctx, agentID, id, "", tags, metadata, 0); err == nil {
				patchWrites++
			}
		}
		result.MemoriesChanged += patchWrites
	}

	routed := s.reconcileRoutedChainFacts(ctx, chainAuth, service.IngestRequest{AgentID: agentID, AppID: appID, SessionID: sessionID}, facts)
	result.MemoriesChanged += routed.memoriesChanged
	result.InsightIDs = append(result.InsightIDs, routed.insightIDs...)
	result.Warnings += routed.warnings
	if result.Status == "" {
		result.Status = "complete"
	}
	return result, nil
}

type listResponse struct {
	Memories []domain.Memory `json:"memories"`
	Total    int             `json:"total"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
}

func (s *Server) listMemories(w http.ResponseWriter, r *http.Request) {
	auth := authInfo(r)
	q := r.URL.Query()
	queries := parseSearchQueries(q)
	rawQuery := ""
	if len(queries) > 0 {
		rawQuery = queries[0]
	}
	contentKeywordSearch := isContentKeywordSearch(q)
	query := strings.TrimSpace(rawQuery)
	if !contentKeywordSearch {
		query = normalizeRecallQuery(rawQuery, time.Now())
		// Normalize the extra facet queries the same way so multi-query
		// recall sees the same temporal rewriting as single-query.
		for i := 1; i < len(queries); i++ {
			queries[i] = normalizeRecallQuery(queries[i], time.Now())
		}
	}

	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 || limit > 200 {
		limit = service.DefaultSessionLimit
	}
	if offset < 0 {
		offset = 0
	}
	appIDFilter, err := parseAppIDFilter(q)
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	var tags []string
	if t := q.Get("tags"); t != "" {
		tags = strings.Split(t, ",")
	}

	strategy, err := parseRetrievalStrategy(q.Get("retrieval_strategy"))
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	filter := domain.MemoryFilter{
		Query:             query,
		Tags:              tags,
		Source:            q.Get("source"),
		State:             q.Get("state"),
		MemoryType:        q.Get("memory_type"),
		AgentID:           q.Get("agent_id"),
		SessionID:         q.Get("session_id"),
		AppID:             appIDFilter,
		SortBy:            q.Get("sort_by"),
		SortDir:           q.Get("sort_dir"),
		Limit:             limit,
		Offset:            offset,
		ScanAll:           parseBoolQuery(q.Get("scanAll")),
		RetrievalStrategy: strategy,
	}
	onlySession := filter.MemoryType == string(domain.TypeSession)

	var memories []domain.Memory
	var total int
	var recallLease *runtimeusage.OperationLease
	recallFinalized := false
	recallSearch := filter.Query != "" && !contentKeywordSearch

	if s.runtimeUsageEnabled() && recallSearch {
		recallLease, err = s.runtimeUsage.BeforeRecall(r.Context(), subjectFromAuth(auth))
		if err != nil {
			s.handleRuntimeUsageError(w, err)
			return
		}
		defer func() {
			if !recallFinalized {
				s.runtimeUsage.AfterRecallFailure(context.Background(), recallLease, context.Canceled)
			}
		}()
	}

	if auth.IsChain() {
		if filter.Query != "" && contentKeywordSearch {
			memories, total, err = s.listChainMemoriesContentKeyword(r.Context(), auth, filter)
		} else {
			memories, total, err = s.listChainMemories(r.Context(), auth, filter)
		}
	} else {
		svc := s.resolveServices(auth)
		switch {
		case filter.Query != "" && contentKeywordSearch:
			memories, total, err = s.listLocalMemoriesContentKeyword(r.Context(), svc, filter)
		case filter.Query != "" && filter.ScanAll:
			memories, total, err = s.listLocalMemoriesScanAll(r.Context(), svc, filter)
		case filter.Query != "" && (filter.MemoryType == "" ||
			filter.MemoryType == string(domain.TypeSession) ||
			filter.MemoryType == string(domain.TypePinned) ||
			filter.MemoryType == string(domain.TypeInsight)):
			// Step 4.4: any query-driven search (regardless of memory_type)
			// routes through svc.memory.Search → recallKV (K=>V path), not
			// the legacy defaultConfidenceRecallSearch / singlePoolConfidence
			// pipelines. Those pipelines hit the old memories table and
			// silently miss data written via the K=>V store path.
			//
			// Multi-query (repeated `q` params) takes the quota-merge
			// variant; the chain / contentKeyword / scanAll branches above
			// deliberately use only the primary query (documented
			// limitation — facet recall is a K=>V capability).
			switch {
			case len(queries) > maxSearchQueries:
				err = &domain.ValidationError{Field: "q", Message: fmt.Sprintf("too many queries (max %d)", maxSearchQueries)}
			case len(queries) > 1:
				normalized := append([]string{filter.Query}, queries[1:]...)
				memories, total, err = svc.memory.SearchMulti(r.Context(), filter, normalized)
			default:
				memories, total, err = svc.memory.Search(r.Context(), filter)
			}
		case onlySession:
			memories, total, err = svc.session.List(r.Context(), filter)
		case !onlySession:
			memories, total, err = svc.memory.Search(r.Context(), filter)
		}
	}

	if err != nil {
		if s.runtimeUsageEnabled() && recallLease != nil {
			s.runtimeUsage.AfterRecallFailure(context.Background(), recallLease, err)
			recallFinalized = true
		}
		s.handleError(r.Context(), w, err)
		return
	}

	if memories == nil {
		memories = []domain.Memory{}
	}
	if !contentKeywordSearch && rawQuery != "" && classifyRecallQueryShape(rawQuery) == recallQueryShapeTime {
		for i := range memories {
			memories[i].Content = service.TemporalRecallProjection(memories[i].Content, memories[i].Metadata)
		}
	}
	if recallSearch {
		if s.runtimeUsageEnabled() && recallLease != nil {
			if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
				return s.runtimeUsage.AfterRecallSuccess(ctx, recallLease, runtimeusage.RecallResult{
					MemoryIDs: memoryIDs(memories),
					AgentName: auth.AgentName,
				})
			}); err != nil {
				s.logger.Error("runtime usage recall finalization failed",
					"operation_id", recallLease.OperationID,
					"tenant_id", auth.TenantID,
					"cluster_id", auth.ClusterID,
					"err", err)
				recallFinalized = true
				s.handleRuntimeUsageError(w, err)
				return
			}
			recallFinalized = true
		}
		s.recordRecallMetering(auth)
	}

	respond(w, http.StatusOK, listResponse{
		Memories: memories,
		Total:    total,
		Limit:    limit,
		Offset:   offset,
	})
}

func isContentKeywordSearch(q url.Values) bool {
	mode := strings.TrimSpace(q.Get("search_mode"))
	if mode == "" {
		mode = strings.TrimSpace(q.Get("searchMode"))
	}
	return strings.EqualFold(mode, "keyword") || strings.EqualFold(mode, "content_keyword")
}

func parseBoolQuery(value string) bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	return err == nil && parsed
}

const maxAppIDLen = 100

// appIDFromCreateRequest resolves the request's app id. When a caller
// sends both `appId` and the legacy `app_id`, `appId` deliberately wins
// — the legacy spelling is only consulted as a fallback.
func appIDFromCreateRequest(req createMemoryRequest) (string, error) {
	raw := req.AppID
	if raw == "" {
		raw = req.AppIDLegacy
	}
	return normalizeAppIDForWrite(raw)
}

func normalizeAppIDForWrite(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) > maxAppIDLen {
		return "", &domain.ValidationError{Field: "appId", Message: "too long (max 100)"}
	}
	return value, nil
}

func parseAppIDFilter(q url.Values) (*string, error) {
	rawValues, ok := q["appId"]
	if !ok {
		rawValues, ok = q["app_id"]
	}
	if !ok {
		return nil, nil
	}
	raw := ""
	if len(rawValues) > 0 {
		raw = rawValues[len(rawValues)-1]
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "null") {
		appID := ""
		return &appID, nil
	}
	if len(raw) > maxAppIDLen {
		return nil, &domain.ValidationError{Field: "appId", Message: "too long (max 100)"}
	}
	return &raw, nil
}

func (s *Server) listLocalMemoriesScanAll(ctx context.Context, svc resolvedSvc, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	switch strings.TrimSpace(filter.MemoryType) {
	case string(domain.TypeSession):
		return svc.session.List(ctx, filter)
	case string(domain.TypePinned), string(domain.TypeInsight):
		return svc.memory.List(ctx, filter)
	case "":
		return s.listLocalAllTypeMemoriesScanAll(ctx, svc, filter)
	default:
		return svc.memory.List(ctx, filter)
	}
}

func (s *Server) listLocalMemoriesContentKeyword(ctx context.Context, svc resolvedSvc, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	switch strings.TrimSpace(filter.MemoryType) {
	case string(domain.TypeSession):
		return svc.session.ContentKeywordSearch(ctx, filter)
	case string(domain.TypePinned), string(domain.TypeInsight):
		return svc.memory.ContentKeywordSearch(ctx, filter)
	case "":
		return s.listLocalAllTypeMemoriesContentKeyword(ctx, svc, filter)
	default:
		return svc.memory.ContentKeywordSearch(ctx, filter)
	}
}

func (s *Server) listLocalAllTypeMemoriesContentKeyword(ctx context.Context, svc resolvedSvc, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	memoryPages, err := collectLocalListPages(ctx, filter, svc.memory.ContentKeywordSearch)
	if err != nil {
		return nil, 0, err
	}
	sessionPages, err := collectLocalListPages(ctx, filter, svc.session.ContentKeywordSearch)
	if err != nil {
		return nil, 0, err
	}

	combined := make([]domain.Memory, 0, len(memoryPages)+len(sessionPages))
	combined = append(combined, memoryPages...)
	combined = append(combined, sessionPages...)
	combined = uniqueChainMemories(combined)

	total := len(combined)
	return finalizeChainMemories(combined, filter, filter.Limit, filter.Offset, false), total, nil
}

func (s *Server) listLocalAllTypeMemoriesScanAll(ctx context.Context, svc resolvedSvc, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	memoryPages, err := collectLocalListPages(ctx, filter, svc.memory.List)
	if err != nil {
		return nil, 0, err
	}
	sessionPages, err := collectLocalListPages(ctx, filter, svc.session.List)
	if err != nil {
		return nil, 0, err
	}

	combined := make([]domain.Memory, 0, len(memoryPages)+len(sessionPages))
	combined = append(combined, memoryPages...)
	combined = append(combined, sessionPages...)
	combined = uniqueChainMemories(combined)

	total := len(combined)
	return finalizeChainMemories(combined, filter, filter.Limit, filter.Offset, false), total, nil
}

func collectLocalListPages(
	ctx context.Context,
	filter domain.MemoryFilter,
	list func(context.Context, domain.MemoryFilter) ([]domain.Memory, int, error),
) ([]domain.Memory, error) {
	pageFilter := filter
	pageFilter.Limit = 200
	pageFilter.Offset = 0

	var all []domain.Memory
	for {
		page, pageTotal, err := list(ctx, pageFilter)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) == 0 || pageFilter.Offset+pageFilter.Limit >= pageTotal {
			return all, nil
		}
		pageFilter.Offset += pageFilter.Limit
	}
}

// maxSearchQueries caps the repeated `q` params on one search request.
// Mirrors the agent's batch tool's effective cap (primary + 4 facet
// variants) so the two layers agree on the contract.
const maxSearchQueries = 5

// parseSearchQueries collects every `q` param, trimmed, empties dropped,
// order-preserving deduped. No cap here: branches that only consume the
// primary query (chain / content-keyword / scanAll) must keep the old
// q.Get("q") semantics of silently ignoring extra values, so the
// maxSearchQueries cap is enforced only where multi-query recall is
// actually taken (the K=>V search branch).
func parseSearchQueries(q url.Values) []string {
	raw := q["q"]
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, r := range raw {
		t := strings.TrimSpace(r)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// parseRetrievalStrategy parses the `retrieval_strategy` query param into a
// uint8 bitmask. Accepts decimal (`31`) or 0x-prefixed hex (`0x1F`). Empty
// string returns 0, which downstream resolves to tidb.StrategyDefaultV1.
//
// Returns a 400-shaped error when the value cannot be parsed or sets bits
// outside the supported 0x1F mask (KEY_EXACT | KEY_FTS | KEY_VEC | VAL_FTS
// | VAL_VEC). Locked by the retrieval-strategy contract.
func parseRetrievalStrategy(value string) (uint8, error) {
	s := strings.TrimSpace(value)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(s, 0, 8)
	if err != nil {
		return 0, &domain.ValidationError{
			Field:   "retrieval_strategy",
			Message: "must be an integer (decimal or 0x-prefixed hex) in the range 0..31",
		}
	}
	const maxMask uint64 = 0x1F
	if n & ^maxMask != 0 {
		return 0, &domain.ValidationError{
			Field:   "retrieval_strategy",
			Message: "bits outside the 0x1F mask (KEY_EXACT|KEY_FTS|KEY_VEC|VAL_FTS|VAL_VEC) are not supported",
		}
	}
	return uint8(n), nil
}

func normalizeRecallQuery(query string, now time.Time) string {
	return service.NormalizeTemporalRecallQuery(query, now)
}

type contentSessionMeta struct {
	Speaker   string `json:"speaker"`
	TurnIndex int    `json:"turn_index"`
}

// func (s *Server) persistContentSession(ctx context.Context, auth *domain.AuthInfo, svc resolvedSvc, sessionID, agentID, content string, metadata json.RawMessage) {
// 	if sessionID == "" || svc.session == nil {
// 		return
// 	}
//
// 	seq, role := contentSessionFields(content, metadata)
// 	if err := svc.session.CreateRawTurn(ctx, sessionID, agentID, auth.AgentName, seq, role, content); err != nil {
// 		slog.Error("content session raw save failed", "cluster_id", auth.ClusterID, "session", sessionID, "err", err)
// 	}
// }

// func contentSessionFields(content string, metadata json.RawMessage) (int, string) {
// 	meta := contentSessionMeta{TurnIndex: -1}
// 	if len(metadata) > 0 {
// 		_ = json.Unmarshal(metadata, &meta)
// 	}
//
// 	role := roleFromSpeaker(meta.Speaker)
// 	if role == "" {
// 		role = roleFromSpeaker(content)
// 	}
// 	if role == "" {
// 		role = "user"
// 	}
//
// 	if meta.TurnIndex >= 0 {
// 		return meta.TurnIndex, role
// 	}
// 	return 0, role
// }

// func roleFromSpeaker(raw string) string {
// 	lower := strings.ToLower(raw)
// 	switch {
// 	case strings.Contains(lower, "speaker 1"), lower == "user":
// 		return "user"
// 	case strings.Contains(lower, "speaker 2"), lower == "assistant", strings.Contains(lower, "assistant"):
// 		return "assistant"
// 	default:
// 		return ""
// 	}
// }

func trimUniqueMemories(mems []domain.Memory, limit int) []domain.Memory {
	if limit <= 0 {
		return []domain.Memory{}
	}

	out := make([]domain.Memory, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, mem := range mems {
		if len(out) >= limit {
			break
		}
		key := mem.Content
		if key == "" {
			key = mem.ID
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, mem)
	}
	return out
}

func (s *Server) getMemory(w http.ResponseWriter, r *http.Request) {
	auth := authInfo(r)
	id := chi.URLParam(r, "id")

	if auth.IsChain() {
		mem, err := s.getChainMemory(r.Context(), auth, id)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
		respond(w, http.StatusOK, mem)
		return
	}

	svc := s.resolveServices(auth)
	mem, err := svc.memory.Get(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		mem, err = svc.session.Get(r.Context(), id)
		if errors.Is(err, domain.ErrNotSupported) {
			err = domain.ErrNotFound
		}
	}
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	// RelativeAge is intentionally absent here — it is query-time only (search endpoint).
	respond(w, http.StatusOK, mem)
}

type updateMemoryRequest struct {
	Content  string          `json:"content,omitempty"`
	Tags     []string        `json:"tags,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func (s *Server) updateMemory(w http.ResponseWriter, r *http.Request) {
	var req updateMemoryRequest
	if err := decode(r, &req); err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	auth := authInfo(r)
	id := chi.URLParam(r, "id")

	var ifMatch int
	if h := r.Header.Get("If-Match"); h != "" {
		ifMatch, _ = strconv.Atoi(h)
	}

	if auth.IsChain() {
		target, err := s.findChainMemoryTarget(r.Context(), auth, id)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
		var lease *runtimeusage.OperationLease
		finalized := false
		if s.runtimeUsageEnabled() {
			lease, err = s.runtimeUsage.BeforeMemoryUpdate(r.Context(), subjectFromAuth(target.nodeAuth))
			if err != nil {
				s.handleRuntimeUsageError(w, err)
				return
			}
			defer func() {
				if !finalized {
					s.runtimeUsage.AfterMemoryUpdateFailure(context.Background(), lease, context.Canceled)
				}
			}()
		}
		mem, err := target.svc.memory.Update(r.Context(), auth.AgentName, id, req.Content, req.Tags, req.Metadata, ifMatch)
		if err != nil {
			if s.runtimeUsageEnabled() {
				s.runtimeUsage.AfterMemoryUpdateFailure(context.Background(), lease, err)
				finalized = true
			}
			s.handleError(r.Context(), w, err)
			return
		}
		mem.ChainSource = target.source
		if s.runtimeUsageEnabled() {
			if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
				return s.runtimeUsage.AfterMemoryUpdateSuccess(ctx, lease, runtimeusage.MemoryUpdateResult{
					MemoryIDs:       []string{mem.ID},
					AgentName:       target.nodeAuth.AgentName,
					ObjectsAffected: 1,
				})
			}); err != nil {
				s.logger.Error("runtime usage chain memory update finalization failed",
					"operation_id", lease.OperationID,
					"tenant_id", target.nodeAuth.TenantID,
					"cluster_id", target.nodeAuth.ClusterID,
					"err", err)
				finalized = true
				s.handleRuntimeUsageError(w, err)
				return
			}
			finalized = true
		}
		go s.afterSuccessfulWrite(target.nodeAuth, target.svc, 1)
		w.Header().Set("ETag", strconv.Itoa(mem.Version))
		respond(w, http.StatusOK, mem)
		return
	}

	svc := s.resolveServices(auth)
	var lease *runtimeusage.OperationLease
	finalized := false
	if s.runtimeUsageEnabled() {
		var err error
		lease, err = s.runtimeUsage.BeforeMemoryUpdate(r.Context(), subjectFromAuth(auth))
		if err != nil {
			s.handleRuntimeUsageError(w, err)
			return
		}
		defer func() {
			if !finalized {
				s.runtimeUsage.AfterMemoryUpdateFailure(context.Background(), lease, context.Canceled)
			}
		}()
	}
	mem, err := svc.memory.Update(r.Context(), auth.AgentName, id, req.Content, req.Tags, req.Metadata, ifMatch)
	if err != nil {
		if s.runtimeUsageEnabled() {
			s.runtimeUsage.AfterMemoryUpdateFailure(context.Background(), lease, err)
			finalized = true
		}
		s.handleError(r.Context(), w, err)
		return
	}
	if s.runtimeUsageEnabled() {
		if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
			return s.runtimeUsage.AfterMemoryUpdateSuccess(ctx, lease, runtimeusage.MemoryUpdateResult{
				MemoryIDs:       []string{mem.ID},
				AgentName:       auth.AgentName,
				ObjectsAffected: 1,
			})
		}); err != nil {
			s.logger.Error("runtime usage memory update finalization failed",
				"operation_id", lease.OperationID,
				"tenant_id", auth.TenantID,
				"cluster_id", auth.ClusterID,
				"err", err)
			finalized = true
			s.handleRuntimeUsageError(w, err)
			return
		}
		finalized = true
	}

	go s.afterSuccessfulWrite(auth, svc, 1)
	w.Header().Set("ETag", strconv.Itoa(mem.Version))
	respond(w, http.StatusOK, mem)
}

func (s *Server) deleteMemory(w http.ResponseWriter, r *http.Request) {
	auth := authInfo(r)
	id := chi.URLParam(r, "id")

	if auth.IsChain() {
		target, err := s.findChainDeleteTarget(r.Context(), auth, id)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
		var lease *runtimeusage.OperationLease
		finalized := false
		if s.runtimeUsageEnabled() {
			lease, err = s.runtimeUsage.BeforeMemoryDelete(r.Context(), subjectFromAuth(target.nodeAuth))
			if err != nil {
				s.handleRuntimeUsageError(w, err)
				return
			}
			defer func() {
				if !finalized {
					s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, context.Canceled)
				}
			}()
		}
		deleted, err := target.svc.memory.Delete(r.Context(), id, auth.AgentName)
		if errors.Is(err, domain.ErrNotFound) {
			deleted, err = target.svc.session.Delete(r.Context(), id, auth.AgentName)
		}
		if err != nil {
			if s.runtimeUsageEnabled() {
				s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, err)
				finalized = true
			}
			s.handleError(r.Context(), w, err)
			return
		}
		if s.runtimeUsageEnabled() {
			if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
				return s.runtimeUsage.AfterMemoryDeleteSuccess(ctx, lease, runtimeusage.MemoryDeleteResult{
					MemoryIDs:       []string{id},
					AgentName:       target.nodeAuth.AgentName,
					ObjectsAffected: deleted,
				})
			}); err != nil {
				s.logger.Error("runtime usage chain memory delete finalization failed",
					"operation_id", lease.OperationID,
					"tenant_id", target.nodeAuth.TenantID,
					"cluster_id", target.nodeAuth.ClusterID,
					"err", err)
				finalized = true
				s.handleRuntimeUsageError(w, err)
				return
			}
			finalized = true
		}
		if deleted > 0 {
			s.enqueueMemoryDeletedWebhook(r.Context(), target.nodeAuth, target.source, auth, id, auth.AgentName)
		}
		go s.afterSuccessfulWrite(target.nodeAuth, target.svc, 0)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	svc := s.resolveServices(auth)
	var lease *runtimeusage.OperationLease
	finalized := false
	if s.runtimeUsageEnabled() {
		var err error
		lease, err = s.runtimeUsage.BeforeMemoryDelete(r.Context(), subjectFromAuth(auth))
		if err != nil {
			s.handleRuntimeUsageError(w, err)
			return
		}
		defer func() {
			if !finalized {
				s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, context.Canceled)
			}
		}()
	}

	deleted, err := svc.memory.Delete(r.Context(), id, auth.AgentName)
	if errors.Is(err, domain.ErrNotFound) {
		deleted, err = svc.session.Delete(r.Context(), id, auth.AgentName)
		if errors.Is(err, domain.ErrNotSupported) {
			err = domain.ErrNotFound
		}
	}
	if err != nil {
		if s.runtimeUsageEnabled() {
			s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, err)
			finalized = true
		}
		s.handleError(r.Context(), w, err)
		return
	}
	if s.runtimeUsageEnabled() {
		if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
			return s.runtimeUsage.AfterMemoryDeleteSuccess(ctx, lease, runtimeusage.MemoryDeleteResult{
				MemoryIDs:       []string{id},
				AgentName:       auth.AgentName,
				ObjectsAffected: deleted,
			})
		}); err != nil {
			s.logger.Error("runtime usage memory delete finalization failed",
				"operation_id", lease.OperationID,
				"tenant_id", auth.TenantID,
				"cluster_id", auth.ClusterID,
				"err", err)
			finalized = true
			s.handleRuntimeUsageError(w, err)
			return
		}
		finalized = true
	}

	if deleted > 0 {
		s.enqueueMemoryDeletedWebhook(r.Context(), auth, nil, nil, id, auth.AgentName)
	}
	go s.afterSuccessfulWrite(auth, svc, 0)
	w.WriteHeader(http.StatusNoContent)
}

type batchDeleteRequest struct {
	IDs []string `json:"ids"`
}

func (s *Server) batchDeleteMemories(w http.ResponseWriter, r *http.Request) {
	var req batchDeleteRequest
	if err := decode(r, &req); err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	auth := authInfo(r)
	if auth.IsChain() {
		groups, err := s.chainDeleteGroups(r.Context(), auth, req.IDs)
		if err != nil {
			if isRuntimeUsageError(err) {
				s.handleRuntimeUsageError(w, err)
			} else {
				s.handleError(r.Context(), w, err)
			}
			return
		}
		var deleted int64
		for _, group := range groups {
			webhookDeleteIDs := s.existingBatchDeleteWebhookIDs(r.Context(), group.target.svc, group.ids)
			var lease *runtimeusage.OperationLease
			finalized := false
			if s.runtimeUsageEnabled() {
				lease, err = s.runtimeUsage.BeforeMemoryDelete(r.Context(), subjectFromAuth(group.target.nodeAuth))
				if err != nil {
					s.handleRuntimeUsageError(w, err)
					return
				}
			}
			groupDeleted, err := group.target.svc.memory.BulkDelete(r.Context(), group.ids, auth.AgentName)
			if err != nil {
				if s.runtimeUsageEnabled() {
					s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, err)
					finalized = true
				}
				s.handleError(r.Context(), w, err)
				return
			}
			sessionDeleted, sessionErr := group.target.svc.session.BulkDelete(r.Context(), group.ids, auth.AgentName)
			if sessionErr != nil && !errors.Is(sessionErr, domain.ErrNotSupported) {
				if s.runtimeUsageEnabled() {
					s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, sessionErr)
					finalized = true
				}
				s.handleError(r.Context(), w, sessionErr)
				return
			}
			groupDeleted += sessionDeleted
			if s.runtimeUsageEnabled() {
				if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
					return s.runtimeUsage.AfterMemoryDeleteSuccess(ctx, lease, runtimeusage.MemoryDeleteResult{
						MemoryIDs:       append([]string(nil), group.ids...),
						AgentName:       group.target.nodeAuth.AgentName,
						ObjectsAffected: groupDeleted,
					})
				}); err != nil {
					s.logger.Error("runtime usage chain batch delete finalization failed",
						"operation_id", lease.OperationID,
						"tenant_id", group.target.nodeAuth.TenantID,
						"cluster_id", group.target.nodeAuth.ClusterID,
						"err", err)
					finalized = true
					s.handleRuntimeUsageError(w, err)
					return
				}
				finalized = true
			}
			if s.runtimeUsageEnabled() && !finalized {
				s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, context.Canceled)
			}
			if groupDeleted > 0 {
				for _, id := range webhookDeleteIDs {
					s.enqueueMemoryDeletedWebhook(r.Context(), group.target.nodeAuth, group.target.source, auth, id, auth.AgentName)
				}
				go s.afterSuccessfulWrite(group.target.nodeAuth, group.target.svc, 0)
			}
			deleted += groupDeleted
		}
		respond(w, http.StatusOK, map[string]any{
			"deleted": deleted,
		})
		return
	}

	svc := s.resolveServices(auth)
	deleteIDs, err := service.ValidateBulkDeleteIDs(req.IDs)
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}
	webhookDeleteIDs := s.existingBatchDeleteWebhookIDs(r.Context(), svc, deleteIDs)
	var lease *runtimeusage.OperationLease
	finalized := false
	if s.runtimeUsageEnabled() {
		var err error
		lease, err = s.runtimeUsage.BeforeMemoryDelete(r.Context(), subjectFromAuth(auth))
		if err != nil {
			s.handleRuntimeUsageError(w, err)
			return
		}
		defer func() {
			if !finalized {
				s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, context.Canceled)
			}
		}()
	}
	deleted, err := svc.memory.BulkDelete(r.Context(), deleteIDs, auth.AgentName)
	if err != nil {
		if s.runtimeUsageEnabled() {
			s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, err)
			finalized = true
		}
		s.handleError(r.Context(), w, err)
		return
	}
	sessionDeleted, sessionErr := svc.session.BulkDelete(r.Context(), deleteIDs, auth.AgentName)
	if sessionErr != nil && !errors.Is(sessionErr, domain.ErrNotSupported) {
		if s.runtimeUsageEnabled() {
			s.runtimeUsage.AfterMemoryDeleteFailure(context.Background(), lease, sessionErr)
			finalized = true
		}
		s.handleError(r.Context(), w, sessionErr)
		return
	}
	deleted += sessionDeleted
	if s.runtimeUsageEnabled() {
		if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
			return s.runtimeUsage.AfterMemoryDeleteSuccess(ctx, lease, runtimeusage.MemoryDeleteResult{
				MemoryIDs:       append([]string(nil), deleteIDs...),
				AgentName:       auth.AgentName,
				ObjectsAffected: deleted,
			})
		}); err != nil {
			s.logger.Error("runtime usage batch delete finalization failed",
				"operation_id", lease.OperationID,
				"tenant_id", auth.TenantID,
				"cluster_id", auth.ClusterID,
				"err", err)
			finalized = true
			s.handleRuntimeUsageError(w, err)
			return
		}
		finalized = true
	}

	if deleted > 0 {
		for _, id := range webhookDeleteIDs {
			s.enqueueMemoryDeletedWebhook(r.Context(), auth, nil, nil, id, auth.AgentName)
		}
	}
	go s.afterSuccessfulWrite(auth, svc, 0)
	respond(w, http.StatusOK, map[string]any{
		"deleted": deleted,
	})
}

func (s *Server) existingBatchDeleteWebhookIDs(ctx context.Context, svc resolvedSvc, ids []string) []string {
	existing := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, err := svc.memory.Get(ctx, id); err == nil {
			existing = append(existing, id)
			continue
		} else if !errors.Is(err, domain.ErrNotFound) {
			s.logger.WarnContext(ctx, "memory delete webhook precheck failed",
				"memory_id", id,
				"err", err,
			)
		}

		if _, err := svc.session.Get(ctx, id); err == nil {
			existing = append(existing, id)
		} else if !errors.Is(err, domain.ErrNotFound) && !errors.Is(err, domain.ErrNotSupported) {
			s.logger.WarnContext(ctx, "session delete webhook precheck failed",
				"memory_id", id,
				"err", err,
			)
		}
	}
	return existing
}

type bulkCreateRequest struct {
	Memories []service.BulkMemoryInput `json:"memories"`
}

func (s *Server) bulkCreateMemories(w http.ResponseWriter, r *http.Request) {
	var req bulkCreateRequest
	if err := decode(r, &req); err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	auth := authInfo(r)
	var writeChainSource *domain.ChainSource
	if auth.IsChain() {
		var err error
		if len(auth.Chain.Nodes) > 0 {
			writeChainSource = chainSource(auth, auth.Chain.Nodes[0])
		}
		auth, err = s.firstChainNodeAuth(auth)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
	}
	svc := s.resolveServices(auth)
	if err := service.ValidateBulkMemoryInputs(req.Memories); err != nil {
		s.handleError(r.Context(), w, err)
		return
	}
	var lease *runtimeusage.OperationLease
	finalized := false
	if s.runtimeUsageEnabled() {
		var err error
		lease, err = s.runtimeUsage.BeforeMemoryCreate(r.Context(), subjectFromAuth(auth), 1)
		if err != nil {
			s.handleRuntimeUsageError(w, err)
			return
		}
		defer func() {
			if !finalized {
				s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, context.Canceled)
			}
		}()
	}
	memories, err := svc.memory.BulkCreate(r.Context(), auth.AgentName, req.Memories)
	if err != nil {
		if s.runtimeUsageEnabled() {
			s.runtimeUsage.AfterMemoryCreateFailure(context.Background(), lease, err)
			finalized = true
		}
		s.handleError(r.Context(), w, err)
		return
	}
	applyChainSource(memories, writeChainSource)
	if s.runtimeUsageEnabled() {
		if err := withRuntimeUsagePostSuccessContext(func(ctx context.Context) error {
			return s.runtimeUsage.AfterMemoryCreateSuccess(ctx, lease, runtimeusage.MemoryCreateResult{
				MemoryIDs:       memoryIDs(memories),
				AgentName:       auth.AgentName,
				ObjectsAffected: int64(len(memories)),
			})
		}); err != nil {
			s.logger.Error("runtime usage bulk create finalization failed",
				"operation_id", lease.OperationID,
				"tenant_id", auth.TenantID,
				"cluster_id", auth.ClusterID,
				"err", err)
			finalized = true
			s.handleRuntimeUsageError(w, err)
			return
		}
		finalized = true
	}

	go s.afterSuccessfulIngest(auth, svc, int64(len(memories)))
	respond(w, http.StatusCreated, map[string]any{
		"ok":       true,
		"memories": memories,
	})
}

func (s *Server) bootstrapMemories(w http.ResponseWriter, r *http.Request) {
	auth := authInfo(r)
	if auth.IsChain() {
		var err error
		auth, err = s.firstChainNodeAuth(auth)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
	}

	svc := s.resolveServices(auth)

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}

	memories, err := svc.memory.Bootstrap(r.Context(), limit)
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	if memories == nil {
		memories = []domain.Memory{}
	}

	respond(w, http.StatusOK, map[string]any{
		"memories": memories,
		"total":    len(memories),
	})
}

func tagsAtIndex(tags [][]string, i int) []string {
	if i < len(tags) && tags[i] != nil {
		return tags[i]
	}
	return []string{}
}

const (
	maxLimitPerSession = 500
	maxSessionIDs      = 100
)

type sessionMessageResponse struct {
	ID          string              `json:"id"`
	SessionID   string              `json:"session_id,omitempty"`
	AgentID     string              `json:"agent_id,omitempty"`
	AppID       string              `json:"appId,omitempty"`
	Source      string              `json:"source,omitempty"`
	Seq         int                 `json:"seq"`
	Role        string              `json:"role"`
	Content     string              `json:"content"`
	ContentType string              `json:"content_type"`
	Tags        []string            `json:"tags"`
	State       domain.MemoryState  `json:"state"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
	ChainSource *domain.ChainSource `json:"chain_source,omitempty"`
}

func (s *Server) handleListSessionMessages(w http.ResponseWriter, r *http.Request) {
	auth := authInfo(r)

	rawIDs := r.URL.Query()["session_id"]
	if len(rawIDs) == 0 {
		s.handleError(r.Context(), w, &domain.ValidationError{
			Field: "session_id", Message: "at least one session_id required",
		})
		return
	}
	sessionIDs := dedupStrings(rawIDs)
	if len(sessionIDs) > maxSessionIDs {
		s.handleError(r.Context(), w, &domain.ValidationError{
			Field: "session_id", Message: "too many session_ids: maximum is 100",
		})
		return
	}
	appIDFilter, err := parseAppIDFilter(r.URL.Query())
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}

	limitPerSession := maxLimitPerSession
	if raw := r.URL.Query().Get("limit_per_session"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			s.handleError(r.Context(), w, &domain.ValidationError{
				Field: "limit_per_session", Message: "must be a positive integer",
			})
			return
		}
		if n < limitPerSession {
			limitPerSession = n
		}
	}

	if auth.IsChain() {
		messages, err := s.listChainSessionMessages(r.Context(), auth, sessionIDs, appIDFilter, limitPerSession)
		if err != nil {
			s.handleError(r.Context(), w, err)
			return
		}
		respond(w, http.StatusOK, map[string]any{
			"messages":          messages,
			"limit_per_session": limitPerSession,
		})
		return
	}

	svc := s.resolveServices(auth)
	sessions, err := svc.session.ListBySessionIDs(r.Context(), sessionIDs, appIDFilter, limitPerSession)
	if err != nil {
		s.handleError(r.Context(), w, err)
		return
	}
	if sessions == nil {
		sessions = []*domain.Session{}
	}
	messages := make([]sessionMessageResponse, len(sessions))
	for i, sess := range sessions {
		messages[i] = sessionMessageResponse{
			ID:          sess.ID,
			SessionID:   sess.SessionID,
			AgentID:     sess.AgentID,
			AppID:       sess.AppID,
			Source:      sess.Source,
			Seq:         sess.Seq,
			Role:        sess.Role,
			Content:     sess.Content,
			ContentType: sess.ContentType,
			Tags:        sess.Tags,
			State:       sess.State,
			CreatedAt:   sess.CreatedAt,
			UpdatedAt:   sess.UpdatedAt,
		}
	}
	respond(w, http.StatusOK, map[string]any{
		"messages":          messages,
		"limit_per_session": limitPerSession,
	})
}

func dedupStrings(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func (s *Server) refreshWriteMetrics(auth *domain.AuthInfo, svc resolvedSvc, written int64) {
	if auth == nil || svc.memory == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clusterID := auth.ClusterID
	if clusterID == "" {
		clusterID = "default"
	}

	if written > 0 {
		metrics.MemoryChangesTotal.WithLabelValues(clusterID).Add(float64(written))
	}

	if s.activity == nil || auth.TenantID == "" {
		logger := s.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("refreshWriteMetrics: activity tracker unavailable", "tenant_id", auth.TenantID, "cluster_id", clusterID)
		return
	}

	observedAt := time.Now().UTC()
	total, last7d, err := svc.memory.CountStats(ctx)
	if err != nil {
		logger := s.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("refreshWriteMetrics: count stats failed", "tenant_id", auth.TenantID, "cluster_id", clusterID, "err", err)
		s.activity.RecordMemoryActivity(auth.TenantID, observedAt)
		return
	}
	s.activity.RecordMemoryStats(ctx, auth.TenantID, observedAt, total, last7d, observedAt)
}
