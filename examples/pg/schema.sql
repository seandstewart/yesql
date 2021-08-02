SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: blog; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA blog;


--
-- Name: unaccent; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;


--
-- Name: EXTENSION unaccent; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION unaccent IS 'text search dictionary that removes accents';


--
-- Name: set_slug_from_title(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_slug_from_title() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        NEW.slug := slugify(NEW.title);
        RETURN NEW;
    END
$$;


--
-- Name: set_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END
$$;


--
-- Name: slugify(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.slugify(value text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
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
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: posts; Type: TABLE; Schema: blog; Owner: -
--

CREATE TABLE blog.posts (
    id bigint NOT NULL,
    title text NOT NULL,
    subtitle text,
    tagline text,
    body text NOT NULL,
    tsv tsvector NOT NULL,
    slug text NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    publication_date date,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: posts_id_seq; Type: SEQUENCE; Schema: blog; Owner: -
--

CREATE SEQUENCE blog.posts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: posts_id_seq; Type: SEQUENCE OWNED BY; Schema: blog; Owner: -
--

ALTER SEQUENCE blog.posts_id_seq OWNED BY blog.posts.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(255) NOT NULL
);


--
-- Name: posts id; Type: DEFAULT; Schema: blog; Owner: -
--

ALTER TABLE ONLY blog.posts ALTER COLUMN id SET DEFAULT nextval('blog.posts_id_seq'::regclass);


--
-- Name: posts posts_pkey; Type: CONSTRAINT; Schema: blog; Owner: -
--

ALTER TABLE ONLY blog.posts
    ADD CONSTRAINT posts_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: post_publication_date; Type: INDEX; Schema: blog; Owner: -
--

CREATE INDEX post_publication_date ON blog.posts USING btree (publication_date);


--
-- Name: post_slug; Type: INDEX; Schema: blog; Owner: -
--

CREATE UNIQUE INDEX post_slug ON blog.posts USING btree (slug);


--
-- Name: post_tags; Type: INDEX; Schema: blog; Owner: -
--

CREATE INDEX post_tags ON blog.posts USING gin (tags);


--
-- Name: post_tsv; Type: INDEX; Schema: blog; Owner: -
--

CREATE INDEX post_tsv ON blog.posts USING gin (tsv);


--
-- Name: posts post_slug_insert; Type: TRIGGER; Schema: blog; Owner: -
--

CREATE TRIGGER post_slug_insert BEFORE INSERT ON blog.posts FOR EACH ROW WHEN ((new.slug IS NULL)) EXECUTE FUNCTION public.set_slug_from_title();


--
-- Name: posts post_slug_update; Type: TRIGGER; Schema: blog; Owner: -
--

CREATE TRIGGER post_slug_update BEFORE UPDATE ON blog.posts FOR EACH ROW WHEN (((new.slug IS NULL) AND (new.title IS NOT NULL) AND (old.title <> new.title))) EXECUTE FUNCTION public.set_slug_from_title();


--
-- Name: posts post_tsv; Type: TRIGGER; Schema: blog; Owner: -
--

CREATE TRIGGER post_tsv BEFORE INSERT OR UPDATE ON blog.posts FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger('tsv', 'pg_catalog.english', 'title', 'body');


--
-- Name: posts post_updated_at; Type: TRIGGER; Schema: blog; Owner: -
--

CREATE TRIGGER post_updated_at BEFORE UPDATE ON blog.posts FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION public.set_updated_at();


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20210802002910');
