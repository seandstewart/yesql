CREATE TABLE IF NOT EXISTS "schema_migrations" (version varchar(255) primary key);
CREATE TABLE posts
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
CREATE INDEX post_publication_date
    ON posts (publication_date)
    WHERE publication_date IS NOT NULL;
CREATE UNIQUE INDEX post_slug
    ON posts (slug)
    WHERE slug IS NOT NULL;
CREATE TRIGGER posts_updated_at
    AFTER UPDATE ON posts
    WHEN NEW.updated_at < OLD.updated_at
    BEGIN
        UPDATE posts SET updated_at = CURRENT_TIMESTAMP WHERE posts.id = NEW.id;
    END;
-- Dbmate schema migrations
INSERT INTO "schema_migrations" (version) VALUES
  ('20211004231716');
