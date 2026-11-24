package action

import (
	"crypto/sha256"
	"strconv"
	"strings"

	"github.com/nexssp/kernel/xerr"
)

// Digest binds a durable execution to its source, input, and contract.
type Digest [sha256.Size]byte

// DigestBytes returns the SHA-256 digest of data.
func DigestBytes(data []byte) Digest {
	return Digest(sha256.Sum256(data))
}

// DigestString returns the SHA-256 digest of value.
func DigestString(value string) Digest {
	return Digest(sha256.Sum256([]byte(value)))
}

// IsZero reports whether d is empty.
func (d Digest) IsZero() bool {
	return d == (Digest{})
}

// Equal reports whether two digests match.
func (d Digest) Equal(other Digest) bool {
	return d == other
}

// ExecutionContractVersion identifies the replay binding contract.
const ExecutionContractVersion = "nexss.execution-contract/v1"

// ExecutionIdentity binds one durable execution to its immutable inputs.
// RequestID remains request-scoped in xctx.
type ExecutionIdentity struct {
	ExecutionID       string
	RootExecutionID   string
	ParentExecutionID string
	Generation        uint64

	SourceDigest   Digest
	InputDigest    Digest
	ContractDigest Digest
}

// SameExecution compares the logical execution namespace.
func (id ExecutionIdentity) SameExecution(other ExecutionIdentity) bool {
	return id.ExecutionID == other.ExecutionID &&
		id.Generation == other.Generation
}

// BindingEqual reports whether durable replay is safe.
func (id ExecutionIdentity) BindingEqual(other ExecutionIdentity) bool {
	return id.SameExecution(other) &&
		id.SourceDigest == other.SourceDigest &&
		id.InputDigest == other.InputDigest &&
		id.ContractDigest == other.ContractDigest
}

const journalNamespaceEscape = "\x00execution-id:"

// JournalNamespace uniquely identifies an execution generation in a journal.
// Generation-zero IDs that overlap generated keys are escaped into a reserved
// form. The returned namespace may contain NUL bytes.
func (id ExecutionIdentity) JournalNamespace() string {
	if id.Generation == 0 {
		// Preserve ordinary keys while escaping IDs that overlap generated keys.
		if needsJournalNamespaceEscape(id.ExecutionID) {
			return journalNamespaceEscape + strconv.Itoa(len(id.ExecutionID)) + ":" + id.ExecutionID + "\x00"
		}
		return id.ExecutionID
	}
	return id.ExecutionID + "\x00generation:" + strconv.FormatUint(id.Generation, 10)
}

func needsJournalNamespaceEscape(executionID string) bool {
	if strings.HasPrefix(executionID, journalNamespaceEscape) {
		return true
	}

	const generationMarker = "\x00generation:"
	_, generationText, found := strings.CutLast(executionID, generationMarker)
	if !found {
		return false
	}
	generation, err := strconv.ParseUint(generationText, 10, 64)
	return err == nil && generation > 0 && strconv.FormatUint(generation, 10) == generationText
}

// Validate checks identity invariants before persistence.
func (id ExecutionIdentity) Validate() error {
	if id.ExecutionID == "" {
		return xerr.Validation(ErrExecutionIDRequired.Error(), ErrExecutionIDRequired)
	}
	if id.Generation > 0 && id.ParentExecutionID == "" {
		return xerr.Validation(ErrParentExecutionIDRequired.Error(), ErrParentExecutionIDRequired)
	}
	return nil
}
