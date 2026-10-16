package action

import (
	"errors"

	"github.com/nexssp/kernel/xerr"
)

// Sentinels remain available through errors.Is; returned errors are xerr-classified.
var (
	ErrExecutionIDRequired        = errors.New("execution identity: execution ID is required")
	ErrParentExecutionIDRequired  = errors.New("execution identity: parent execution ID is required for a non-zero generation")
	ErrExecutionNamespaceMismatch = errors.New("execution identity: execution namespace mismatch")
	ErrSourceDigestMismatch       = errors.New("execution identity: source digest mismatch")
	ErrInputDigestMismatch        = errors.New("execution identity: input digest mismatch")
	ErrContractDigestMismatch     = errors.New("execution identity: contract digest mismatch")
)

// ValidateBinding returns Conflict when durable replay is unsafe.
func (id ExecutionIdentity) ValidateBinding(other ExecutionIdentity) error {
	if !id.SameExecution(other) {
		return xerr.Conflict(ErrExecutionNamespaceMismatch.Error(), ErrExecutionNamespaceMismatch)
	}
	if id.SourceDigest != other.SourceDigest {
		return xerr.Conflict(ErrSourceDigestMismatch.Error(), ErrSourceDigestMismatch)
	}
	if id.InputDigest != other.InputDigest {
		return xerr.Conflict(ErrInputDigestMismatch.Error(), ErrInputDigestMismatch)
	}
	if id.ContractDigest != other.ContractDigest {
		return xerr.Conflict(ErrContractDigestMismatch.Error(), ErrContractDigestMismatch)
	}
	return nil
}
