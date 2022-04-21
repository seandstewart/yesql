-- :name get :one
-- Get a blog post by id.
SELECT * FROM blog.posts WHERE id = :id;

-- :name get_by_slug :one
-- Get a blog post by slug.
SELECT * FROM blog.posts WHERE slug = :slug;

-- :name get_by_tags :many
-- Get blog posts matching the given tags.
SELECT * FROM blog.posts WHERE tags && :tags::text[];

-- :name all :many
-- Get all blog posts.
SELECT * FROM blog.posts ORDER BY id;

-- :name published :many
-- Get all blog posts which have been published up to this date.
SELECT * FROM blog.posts where publication_date <= coalesce(:date, current_date);

-- :name search :many
-- Search all blog posts using full-text a generalized word search.
SELECT * FROM blog.posts
WHERE tsv @@ :words;

-- :name search_phrase :many
-- Search all blog posts for a particular phrase.
SELECT *
FROM blog.posts
WHERE tsv @@ phraseto_tsquery(:phrase);
