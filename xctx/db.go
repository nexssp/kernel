package xctx

// TenantDB is the context key for the active tenant's DB connection or transaction handle.
var TenantDB = NewKey[any]("tenant_db")
