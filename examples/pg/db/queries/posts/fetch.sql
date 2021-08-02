-- name: get^
-- Get a blog post by id.
SELECT * FROM blog.posts WHERE id = :id;

-- name: get_by_slug^
-- Get a blog post by slug.
SELECT * FROM blog.posts WHERE slug = :slug;

-- name: get_by_tags
-- Get blog posts matching the given tags.
SELECT * FROM blog.posts WHERE tags && :tags::text[];

-- name: all
-- Get all blog posts.
SELECT * FROM blog.posts;
