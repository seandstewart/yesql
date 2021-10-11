-- name: get^
-- Get a blog post by id.
SELECT * FROM posts WHERE id = :id;

-- name: get_by_slug^
-- Get a blog post by slug.
SELECT * FROM posts WHERE slug = :slug;

-- name: all
-- Get all blog posts.
SELECT * FROM posts;

-- name: published
-- Get all blog posts which have been published up to this date.
SELECT * FROM posts where publication_date <= coalesce(:date, current_date);

