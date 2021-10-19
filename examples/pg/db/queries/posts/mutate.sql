-- name: create<!
-- Create a new blog post :)
INSERT INTO blog.posts (
    title,
    subtitle,
    tagline,
    tags,
    body,
    publication_date
)
VALUES (
    :title,
    :subtitle,
    :tagline,
    :tags,
    :body,
    :publication_date
)
RETURNING *;

-- name: bulk_create*!
-- Create a new blog post :)
INSERT INTO blog.posts (
    title,
    subtitle,
    tagline,
    tags,
    body,
    publication_date
)
VALUES (
    :title,
    :subtitle,
    :tagline,
    :tags,
    :body,
    :publication_date
);

-- name: bulk_create_returning
-- Create a new blog post :)
WITH new_posts AS (
    SELECT
        title,
        subtitle,
        tagline,
        body,
        coalesce(tags, '{}'::text[]) as tags,
        publication_date
    FROM unnest(:posts::blog.new_post[])
    AS t(
         title,
         subtitle,
         tagline,
         body,
         tags,
         publication_date
    )
)
INSERT INTO blog.posts (
    title,
    subtitle,
    tagline,
    body,
    tags,
    publication_date
) SELECT * FROM new_posts
RETURNING *;

-- name: update<!
-- Update a post with all new data.
UPDATE blog.posts
SET
    title = :title,
    subtitle = :subtitle,
    tagline = :tagline,
    tags = :tags,
    body = :body,
    publication_date = :publication_date
WHERE id = :id
RETURNING *;

-- name: delete<!
-- Delete a post.
DELETE FROM blog.posts WHERE id = :id RETURNING *;

-- name: publish<!
-- Set the publication date for a blog post.
UPDATE blog.posts
SET publication_date = coalesce(:publication_date::date, CURRENT_DATE)
WHERE id = :id
RETURNING *;

-- name: retract<!
-- "Retract" a blog post by clearing out the publication_date.
UPDATE blog.posts
SET publication_date = null
WHERE id = :id
RETURNING *;

-- name: add_tags<!
-- Add new tags for a blog post.
UPDATE blog.posts
SET tags = (select array(select distinct unnest(tags || :tags::text[])))
WHERE id = :id
RETURNING tags;

-- name: remove_tags<!
-- Remove tags for this blog post
UPDATE blog.posts
SET tags = (select array(select unnest(tags) except select unnest(:tags::text[])))
WHERE id = :id
RETURNING tags;

-- name: clear_tags!
UPDATE blog.posts
SET tags = '{}'::text[]
WHERE id = :id;

-- name: set_body!
UPDATE blog.posts
SET body = :body
WHERE id = :id;
