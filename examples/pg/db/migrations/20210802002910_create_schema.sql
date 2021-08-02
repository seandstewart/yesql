-- migrate:up
CREATE EXTENSION IF NOT EXISTS unaccent;

/* Create public functions */

-- https://gist.github.com/kez/17638bade0382f820280dafa46277435
CREATE OR REPLACE FUNCTION slugify("value" TEXT) RETURNS TEXT
    LANGUAGE SQL STRICT IMMUTABLE AS
$$
    -- removes accents (diacritic signs) from a given string --
    WITH "unaccented" AS (
        SELECT unaccent("value") AS "value"
    ),
     -- lowercases the string
     "lowercase" AS (
         SELECT lower("value") AS "value"
         FROM "unaccented"
     ),
     -- remove single and double quotes
     "removed_quotes" AS (
         SELECT regexp_replace("value", '[''"]+', '', 'gi') AS "value"
         FROM "lowercase"
     ),
     -- replaces anything that's not a letter, number, hyphen('-'), or underscore('_') with a hyphen('-')
     "hyphenated" AS (
         SELECT regexp_replace("value", '[^a-z0-9\\-_]+', '-', 'gi') AS "value"
         FROM "removed_quotes"
     ),
     -- trims hyphens('-') if they exist on the head or tail of the string
     "trimmed" AS (
         SELECT regexp_replace(regexp_replace("value", '\-+$', ''), '^\-', '') AS "value"
         FROM "hyphenated"
     )
    SELECT "value"
    FROM "trimmed";
$$;

CREATE OR REPLACE FUNCTION get_updated_at() RETURNS TIMESTAMPTZ
    LANGUAGE SQL STRICT IMMUTABLE AS
$$
    SELECT CURRENT_TIMESTAMP;
$$;

/* Create schema */

CREATE SCHEMA IF NOT EXISTS blog;

CREATE TABLE IF NOT EXISTS blog.posts (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    title TEXT,
    slug TEXT GENERATED ALWAYS AS (slugify(title)) STORED,
    subtitle TEXT,
    tagline TEXT,
    body TEXT,
    tags TEXT[] NOT NULL DEFAULT '{}'::text[],
    publication_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ GENERATED ALWAYS AS ( get_updated_at() ) STORED,
    tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(subtitle, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(tagline, '')), 'C') ||
        setweight(to_tsvector('english', coalesce(body, '')), 'D')
    ) STORED
);

CREATE INDEX IF NOT EXISTS post_publication_date
    ON blog.posts (publication_date)
    WHERE publication_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS post_tags ON blog.posts USING GIN (tags);
CREATE INDEX IF NOT EXISTS post_tsv ON blog.posts USING GIN (tsv);
CREATE UNIQUE INDEX IF NOT EXISTS post_slug
    ON blog.posts (slug)
    WHERE slug IS NOT NULL;

-- migrate:down

DROP SCHEMA IF EXISTS blog CASCADE;
