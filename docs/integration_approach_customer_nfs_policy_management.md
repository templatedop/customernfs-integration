# Integration Approach: Customer NFS → Policy Management

## 1. Overview

Customer NFS handles name change, address change, mobile number change, and email change requests.
Policy Management (PM) is the central orchestrator with a per-policy long-running `PolicyLifecycleWorkflow` (`plw-{policy_number}`).

**Direction of flow**: Customer NFS → Policy Management (one-way notification).
PM does NOT initiate NFS requests. NFS owns the full request lifecycle and notifies PM on completion.

---

## 2. What Changes Are Covered

| Change Type      | NFS Workflows                                              | PM Signal |
|------------------|------------------------------------------------------------|-----------|
| Address Change   | WF-NFS-001 (Aadhaar), WF-NFS-002 (Manual)                | `customer-nfr-completed` |
| Name Change      | WF-NFS-003 (Aadhaar), WF-NFS-004 (Manual)                | `customer-nfr-completed` |
| Mobile Change    | WF-NFS-006 (New — to be created)                          | `customer-nfr-completed` |
| Email Change     | WF-NFS-007 (New — to be created)                          | `customer-nfr-completed` |

All four change types use the **same signal channel** `customer-nfr-completed` to PM.
The `RequestType` field inside the signal payload differentiates them.

---

## 3. Signal Contract

### 3.1 New Signal: `customer-nfr-completed`

This is a **new dedicated signal channel** in PM, separate from the existing `nfr-completed`.
The existing `nfr-completed` handles PM-initiated NFRs (assignment, nomination, etc.).
`customer-nfr-completed` handles externally-initiated customer NFS changes.

**Signal Payload** (reuses PM's existing `OperationCompletedSignal` structure):

```go
type OperationCompletedSignal struct {
    RequestID       string          `json:"request_id"`
    RequestType     string          `json:"request_type"`       // ADDRESS_CHANGE, NAME_CHANGE, MOBILE_CHANGE, EMAIL_CHANGE
    Outcome         string          `json:"outcome"`            // APPROVED, REJECTED
    StateTransition string          `json:"state_transition"`   // empty — no policy state change
    OutcomePayload  json.RawMessage `json:"outcome_payload"`    // changed field values
    CompletedAt     time.Time       `json:"completed_at"`
}
```

**Target Workflow ID**: `plw-{policy_number}` (one signal per affected policy)
**RunID**: `""` (empty — targets latest run, handles Continue-As-New transparently)

### 3.2 OutcomePayload Examples

**ADDRESS_CHANGE:**
```json
{
    "address_type": "COMMUNICATION",
    "pin_code": "110001",
    "state": "DELHI",
    "district": "NEW DELHI",
    "city": "NEW DELHI"
}
```

**NAME_CHANGE:**
```json
{
    "salutation": "Mr",
    "first_name": "Rajesh",
    "middle_name": "Kumar",
    "last_name": "Sharma"
}
```

**MOBILE_CHANGE:**
```json
{
    "mobile_number": "9876543210"
}
```

**EMAIL_CHANGE:**
```json
{
    "email": "rajesh.sharma@example.com"
}
```

---

## 4. Architecture: Signal Flow

```
Customer → NFS HTTP API → NFS Workflow (processes change)
                                │
                                │ On COMPLETED / REJECTED:
                                │
                                ├─► Activity: LookupAffectedPolicies(customerID)
                                │   Returns: ["PLI/2026/000001", "PLI/2026/000002", ...]
                                │
                                └─► Activity: NotifyPolicyManagement
                                    For EACH policy:
                                      SignalExternalWorkflow(
                                          "plw-{policy_number}",
                                          "",
                                          "customer-nfr-completed",
                                          OperationCompletedSignal{...}
                                      )
                                            │
                                            ▼
                              PolicyLifecycleWorkflow (plw-{policy_number})
                                            │
                                            ├─► handleCustomerNFRCompleted()
                                            ├─► Dedup via ProcessedSignalIDs
                                            ├─► Log signal to policy_signal_log
                                            ├─► Update policy metadata (allow-listed keys only)
                                            └─► No state transition (policy status unchanged)
```

**Key**: This is NOT a policy state change. No financial lock is involved. The handler
only updates metadata fields on the policy record.

---

## 5. Changes: Customer NFS Service

### 5.1 CustomerID: string (UUID) → int64 (BIGINT)

**Rationale**: PM uses `int64` for CustomerID (`PolicyMetadata.CustomerID` is `int64`).
The NFS codebase currently uses `string` with UUID validation. This must be aligned.

**Blast Radius** (36 locations across 6 layers):

| Layer | Files | Count |
|-------|-------|-------|
| Domain models | `service_request.go`, `address_change_detail.go`, `name_change_detail.go` | 3 structs |
| Activity types | `temporal/activities/types.go` | 9 input/output types |
| Workflow inputs | `address_change_workflow.go`, `name_change_workflow.go` | 2 workflow inputs + signal payload |
| Request DTOs | `handler/request.go` | 5 request definitions |
| Response DTOs | `handler/response/cpc.go`, `status.go` | 3 response structs |
| DB repos | `service_request.go`, `address_change.go`, `name_change.go` | 18+ SQL references |
| DB schema | `migrations/001_create_nfs_schema.sql` | Domain `nfs.customer_id` currently `UUID` |

**Migration approach**:
1. Change domain type: `CREATE DOMAIN nfs.customer_id AS BIGINT;` (new migration)
2. Change all Go struct fields: `CustomerID string` → `CustomerID int64`
3. Remove UUID validation: `validate:"required,uuid"` → `validate:"required,gt=0"`
4. Update JSON serialization: numeric instead of string
5. Update all repo functions that accept `customerID string` → `customerID int64`

### 5.2 New Activity: `NotifyPolicyManagement`

**File**: `temporal/activities/pm_notification_activity.go`

```go
type NotifyPMInput struct {
    RequestID     string          `json:"request_id"`
    CustomerID    int64           `json:"customer_id"`
    RequestType   string          `json:"request_type"`    // ADDRESS_CHANGE, NAME_CHANGE, MOBILE_CHANGE, EMAIL_CHANGE
    Outcome       string          `json:"outcome"`         // APPROVED, REJECTED
    ChangePayload json.RawMessage `json:"change_payload"`
}
```

**Responsibilities**:
1. Call an activity `LookupAffectedPolicies` to fetch all active policy numbers for the customer
   (via PM API or a shared read-only view of the policy table).
2. For each policy, call `temporalClient.SignalWorkflow()` to `plw-{policy_number}` with
   `customer-nfr-completed` signal and `OperationCompletedSignal` payload.
3. Retry policy: 3 attempts with 2s initial interval (Temporal activity retry).
4. Errors are logged but do NOT fail the NFS workflow (fire-and-forget semantics).

### 5.3 New PM Integration Types

**File**: `temporal/workflows/pm_types.go`

```go
// Matches PM's OperationCompletedSignal contract exactly.
type OperationCompletedSignal struct {
    RequestID       string          `json:"request_id"`
    RequestType     string          `json:"request_type"`
    Outcome         string          `json:"outcome"`
    StateTransition string          `json:"state_transition,omitempty"`
    OutcomePayload  json.RawMessage `json:"outcome_payload,omitempty"`
    CompletedAt     time.Time       `json:"completed_at"`
}

const SignalCustomerNFRCompleted = "customer-nfr-completed"
```

### 5.4 Workflow Changes (Existing — 4 workflows, ~6 terminal points)

At each terminal point (COMPLETED or REJECTED) in the existing workflows, add a call to
`NotifyPolicyManagement` activity **after** the status update:

**WF-NFS-001** (`AadhaarAddressChangeWorkflow`):
- After line 597 (status → COMPLETED)

**WF-NFS-002** (`ManualAddressChangeWorkflow`):
- After line 732 (APPROVE → COMPLETED)
- After line 744 (REJECT → REJECTED)

**WF-NFS-003** (`AadhaarNameChangeWorkflow`):
- After line 195 (status → COMPLETED)

**WF-NFS-004** (`ManualNameChangeWorkflow`):
- After line 394 (APPROVE → COMPLETED)
- After REJECT terminal point

### 5.5 New Workflows: Mobile Change and Email Change

**WF-NFS-006: Mobile Number Change** (`MobileChangeWorkflow`)
- Similar pattern to Aadhaar address change (OTP-based verification)
- Flow: Initiate → Send OTP to new mobile → Verify OTP → Update mobile → COMPLETED → Notify PM
- New handler endpoint: `POST /nfs/mobile-change/initiate`
- New handler endpoint: `POST /nfs/mobile-change/:id/verify-otp`

**WF-NFS-007: Email Change** (`EmailChangeWorkflow`)
- Flow: Initiate → Send verification link/OTP to new email → Verify → Update email → COMPLETED → Notify PM
- New handler endpoint: `POST /nfs/email-change/initiate`
- New handler endpoint: `POST /nfs/email-change/:id/verify`

Both follow the same completion pattern: call `NotifyPolicyManagement` activity on COMPLETED/REJECTED.

### 5.6 New Domain Models and DB Tables

**Mobile Change**:
- `domain.MobileChangeDetail` struct
- `nfs.mobile_change_detail` table
- `nfs.mobile_version_history` table

**Email Change**:
- `domain.EmailChangeDetail` struct
- `nfs.email_change_detail` table
- `nfs.email_version_history` table

---

## 6. Changes: Policy Management Service

### 6.1 New Signal Constant

**File**: `workflows/signals.go`

```go
SignalCustomerNFRCompleted = "customer-nfr-completed" // From Customer NFS service
```

### 6.2 New Signal Channel Registration

**File**: `workflows/policy_lifecycle_workflow.go` (main select loop, ~line 525)

```go
customerNfrCompletedCh := workflow.GetSignalChannel(ctx, SignalCustomerNFRCompleted)
```

And in the selector (~line 714):

```go
sel.AddReceive(customerNfrCompletedCh, func(c workflow.ReceiveChannel, _ bool) {
    var sig OperationCompletedSignal
    c.Receive(ctx, &sig)
    workflow.GetLogger(ctx).Info("Customer NFR Completed signal received",
        "RequestID", sig.RequestID,
        "RequestType", sig.RequestType,
        "Outcome", sig.Outcome,
    )
    handleCustomerNFRCompleted(ctx, &state, sig)
})
```

### 6.3 New Handler: `handleCustomerNFRCompleted`

**File**: `workflows/policy_lifecycle_workflow.go`

This handler is distinct from `handleNFRCompleted` because:
- It does NOT require a matching `PendingRequest` (NFS initiated externally, not via PM)
- It does NOT update PM's `service_request` table (PM has no service_request for this)
- It ONLY updates policy metadata and logs the signal

```go
func handleCustomerNFRCompleted(ctx workflow.Context, state *PolicyLifecycleState, sig OperationCompletedSignal) {
    // 1. Dedup check
    if _, seen := state.ProcessedSignalIDs[sig.RequestID]; seen {
        return
    }

    // 2. Log signal received
    payload, _ := json.Marshal(sig)
    _ = workflow.ExecuteActivity(shortActCtx(ctx),
        policyActs.LogSignalReceivedActivity,
        acts.SignalLogEntry{
            PolicyID:      state.PolicyDBID,
            SignalChannel: SignalCustomerNFRCompleted,
            SignalPayload: payload,
            RequestID:     sig.RequestID,
            SourceService: "customer-nfs",
            Status:        domain.SignalStatusProcessed,
        }).Get(ctx, nil)

    // 3. Update policy metadata (only if APPROVED and payload present)
    if sig.Outcome == "APPROVED" && sig.OutcomePayload != nil {
        var p map[string]interface{}
        if json.Unmarshal(sig.OutcomePayload, &p) == nil {
            filtered := filterCustomerNFRPayload(sig.RequestType, p)
            if len(filtered) > 0 {
                _ = workflow.ExecuteActivity(shortActCtx(ctx),
                    policyActs.UpdatePolicyMetadataActivity,
                    acts.MetadataUpdateParams{
                        PolicyID: state.PolicyDBID,
                        Updates:  filtered,
                    }).Get(ctx, nil)
            }
        }
    }

    // 4. Mark as processed for dedup
    state.ProcessedSignalIDs[sig.RequestID] = workflow.Now(ctx)
}
```

### 6.4 New Metadata Allow-List for Customer NFS

Separate from existing `nfrMetadataAllowList` to maintain security isolation:

```go
var customerNFRMetadataAllowList = map[string]map[string]bool{
    "ADDRESS_CHANGE": {
        "address_type": true,
        "pin_code":     true,
        "state":        true,
        "district":     true,
        "city":         true,
    },
    "NAME_CHANGE": {
        "salutation":  true,
        "first_name":  true,
        "middle_name": true,
        "last_name":   true,
    },
    "MOBILE_CHANGE": {
        "mobile_number": true,
    },
    "EMAIL_CHANGE": {
        "email": true,
    },
}
```

### 6.5 New Request Type Constants (if not already present)

**File**: `core/domain/service_request.go`

```go
RequestTypeMobileChange = "MOBILE_CHANGE"
RequestTypeEmailChange  = "EMAIL_CHANGE"
```

---

## 7. What Is NOT Changed

| Aspect | Reason |
|--------|--------|
| PM's existing `handleNFRCompleted` | Handles PM-initiated NFRs (assignment, nomination). Untouched. |
| PM's `handleNFRRequest` | Not involved — PM does not initiate customer NFS requests. |
| PM's `nfr-request` / `nfr-completed` signals | Existing signals for PM-initiated flow. Untouched. |
| PM's `PolicyLifecycleState.CurrentStatus` | Customer NFS changes do not trigger policy state transitions. |
| PM's `FinancialLock` | Customer NFS changes are non-financial. No lock involved. |
| NFS's existing HTTP endpoints | Remain functional as-is. PM notification is additive. |

---

## 8. Implementation Order

| Step | Description | Service |
|------|-------------|---------|
| 1 | CustomerID string → int64 migration (domain, activities, workflows, handlers, repos, DB) | Customer NFS |
| 2 | Add PM integration types (`OperationCompletedSignal`, signal constant) | Customer NFS |
| 3 | Add `LookupAffectedPolicies` activity | Customer NFS |
| 4 | Add `NotifyPolicyManagement` activity | Customer NFS |
| 5 | Add `NotifyPolicyManagement` call to WF-NFS-001 through WF-NFS-004 terminal points | Customer NFS |
| 6 | Implement WF-NFS-006 (Mobile Change) with PM notification | Customer NFS |
| 7 | Implement WF-NFS-007 (Email Change) with PM notification | Customer NFS |
| 8 | Add `SignalCustomerNFRCompleted` constant | Policy Management |
| 9 | Register `customer-nfr-completed` signal channel in PLW select loop | Policy Management |
| 10 | Implement `handleCustomerNFRCompleted` handler | Policy Management |
| 11 | Add `customerNFRMetadataAllowList` and `filterCustomerNFRPayload` | Policy Management |
| 12 | Add `MOBILE_CHANGE` and `EMAIL_CHANGE` request type constants | Policy Management |

---

## 9. Key Design Decisions

1. **Separate signal channel (`customer-nfr-completed`)** — Keeps externally-initiated (Customer NFS)
   and internally-initiated (PM → downstream) NFR flows isolated. Different handlers, different
   trust boundaries, different metadata allow-lists.

2. **Activity-based notification (not inline `SignalExternalWorkflow`)** — The notification involves
   DB lookup (affected policies) + multiple signal calls. An activity provides retry semantics,
   timeout handling, and keeps workflow code clean.

3. **Fan-out to all policies** — A customer may hold multiple policies. Each `plw-{policy_number}`
   is an independent workflow. The activity signals each one individually.

4. **Fire-and-forget from NFS** — NFS workflow completion is not blocked by PM notification failure.
   The activity call uses `Get(ctx, nil)` and errors are logged but do not fail the workflow.

5. **Empty RunID** — Using `""` for RunID in `SignalExternalWorkflow` targets the latest workflow run.
   This transparently handles PM's Continue-As-New pattern (40K events or 30 days).

6. **Dedup** — PM's `ProcessedSignalIDs` (90-day TTL) prevents duplicate processing. NFS uses
   the same `RequestID` for all policy signals from one change, but each PLW deduplicates independently.

7. **No PendingRequest** — Unlike PM-initiated NFRs, customer NFS changes don't create a
   PendingRequest in PLW. The handler directly processes the signal.

---

## 10. Open Questions

1. **Policy lookup**: How does NFS discover all active policies for a customer?
   - Option A: Direct DB read from a shared read-only policy view
   - Option B: Call PM's API/query handler to get policies by customer_id
   - Option C: Use a dedicated "customer-policy-mapping" service

2. **Mobile/Email schema**: Are mobile number and email stored on the `policy` table in PM,
   or only in the customer service? This determines whether `UpdatePolicyMetadataActivity` is needed
   or if the signal is purely for audit/logging.

3. **Backward compatibility**: The CustomerID UUID → BIGINT migration requires a new DB migration.
   Existing data in `nfs.service_request` will need a data migration strategy.
