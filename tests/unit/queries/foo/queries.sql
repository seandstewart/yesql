-- name: get
-- Get a foo by id.
SELECT *
FROM tests.foo
WHERE id = :id;
