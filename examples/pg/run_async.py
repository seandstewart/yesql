import yesql.dynamic

from db import client, model


async def run():
    posts = client.AsyncPosts()
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    await posts.initialize()
    persisted = await posts.create(**posts.get_kvs(post))
    print(f"Created a post: {persisted!r}")
    matches = await posts.search(words="super & great")
    print(f"Found matches for keywords: {matches!r}")
    matches = await posts.search_phrase(phrase="super great")
    print(f"Found matches for phrase: {matches!r}")
    matches = await posts.get_by_tags(tags={"tips", "tricks"})
    print(f"Found matches for tags: {matches!r}")
    persisted.title = "Maybe It's Not So Great After All"
    updated = await posts.update(**posts.get_kvs(post), id=persisted.id)
    print(f"Updated a post: {updated!r}")
    deleted = await posts.delete(id=persisted.id)
    print(f"Deleted a post: {deleted!r}")
    await posts.bulk_create(params=[posts.get_kvs(post)])
    created = await posts.all()
    print(f"Bulk-created posts: {created}")
    await posts.delete(id=created[0].id)
    created = await posts.bulk_create_returning([post])
    print(f"Bulk-created (returning) posts: {created}")
    await posts.delete(id=created[0].id)


async def dynamic():
    posts = client.AsyncPosts()
    await posts.initialize()
    dyn = yesql.dynamic.DynamicQueryService(posts, schema="blog")
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    persisted = await posts.create(**posts.get_kvs(post))
    found = await dyn.select(title=persisted.title)
    print(f"Dynamically selected posts: {found}")
    q = dyn.table.select(dyn.table.id).where(dyn.table.title == persisted.title)
    id = await dyn.execute(q, modifier="scalar")
    print(f"Dynamically for Post ID with {post.title=}: {id=}")
    await posts.delete(id=persisted.id)


if __name__ == "__main__":
    import asyncio
    import os

    os.environ[
        "database_url"
    ] = "postgres://postgres:@localhost:5432/blog?sslmode=disable"
    asyncio.run(run())
    asyncio.run(dynamic())
