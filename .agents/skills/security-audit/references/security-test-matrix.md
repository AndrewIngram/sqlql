# Security Test Matrix

Map each confirmed finding to at least one regression test.

## 1) Auth Boundary and Scope

Scenarios:
- reject cross-tenant data access attempts
- enforce context-derived scopes across primary and fallback paths
- verify absent/invalid auth context fails closed

Expected assertions:
- unauthorized rows never returned
- strict errors on out-of-scope requests

## 2) Data Exposure / Projection Controls

Scenarios:
- select/filter/order on undeclared fields must fail
- wildcard or alias projections cannot leak hidden fields
- joins cannot reintroduce sensitive undeclared columns

Expected assertions:
- deterministic errors for undeclared field references
- only declared facade columns in output

## 3) Injection Safety

Scenarios:
- malicious expression fragments in public inputs
- delimiter/comment/operator abuse attempts
- malformed AST fragments that should be rejected

Expected assertions:
- input rejected or safely parameterized
- no altered query semantics from attacker strings

## 4) Availability Guardrails

Scenarios:
- attacker-controlled large cardinality requests
- expensive sort/join/aggregate fallback paths
- repeated batch amplification attempts

Expected assertions:
- guardrails trigger before expensive execution where possible
- stable failure mode with clear error messages

## 5) No-Finding Residual Validation

When no vulnerabilities are found, still add confidence checks:
- boundary mapping completed
- high-risk paths exercised by tests
- residual risk and test gaps documented
