-- migrate:up

/* Create schema */

CREATE TABLE IF NOT EXISTS posts
(
    id               INTEGER PRIMARY KEY,
    title            TEXT,
    slug             TEXT,
    subtitle         TEXT,
    tagline          TEXT,
    body             TEXT,
    publication_date DATE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS post_publication_date
    ON posts (publication_date)
    WHERE publication_date IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS post_slug
    ON posts (slug)
    WHERE slug IS NOT NULL;
CREATE TRIGGER IF NOT EXISTS posts_updated_at
    AFTER UPDATE ON posts
    WHEN NEW.updated_at < OLD.updated_at
    BEGIN
        UPDATE posts SET updated_at = CURRENT_TIMESTAMP WHERE posts.id = NEW.id;
    END;


-- migrate:down

DROP INDEX IF EXISTS post_slug;
DROP INDEX IF EXISTS post_publication_date;
DROP TABLE IF EXISTS posts;
