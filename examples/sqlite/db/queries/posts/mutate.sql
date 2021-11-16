-- name: create<!
-- Create a new blog post :)
INSERT INTO posts (
    title,
    slug,
    subtitle,
    tagline,
    body,
    publication_date
)
VALUES (
    :title,
    :slug,
    :subtitle,
    :tagline,
    :body,
    :publication_date
)
RETURNING *;

-- name: bulk_create*!
-- Create a new blog post :)
INSERT INTO posts (
    title,
    slug,
    subtitle,
    tagline,
    body,
    publication_date
)
VALUES (
    :title,
    :slug,
    :subtitle,
    :tagline,
    :body,
    :publication_date
);

-- name: update<!
-- Update a post with all new data.
UPDATE posts
SET
    title = :title,
    slug = :slug,
    subtitle = :subtitle,
    tagline = :tagline,
    body = :body,
    publication_date = :publication_date
WHERE id = :id
RETURNING *;

-- name: delete<!
-- Delete a post.
DELETE FROM posts WHERE id = :id RETURNING *;

-- name: publish<!
-- Set the publication date for a blog post.
UPDATE posts
SET publication_date = coalesce(:publication_date, CURRENT_DATE)
WHERE id = :id
RETURNING *;

-- name: retract<!
-- "Retract" a blog post by clearing out the publication_date.
UPDATE posts
SET publication_date = null
WHERE id = :id
RETURNING *;

-- name: set_body<!
UPDATE posts
SET body = :body
WHERE id = :id
RETURNING *;
