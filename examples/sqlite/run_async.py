import yesql.dynamic
from db import client, model


async def run():
    posts = client.AsyncPosts()
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
    )
    persisted = await posts.create(model=post)
    print(f"Created a post: {persisted!r}")
    fetched = await posts.get(id=persisted.id)
    print(f"Fetched a post: {fetched!r}")
    fetched.title = "Maybe It's Not So Great After All"
    updated = await posts.update(model=fetched, id=fetched.id)
    print(f"Updated a post: {updated!r}")
    deleted = await posts.delete(id=fetched.id)
    print(f"Deleted a post: {deleted!r}")


async def dynamic():
    posts = client.AsyncPosts()
    dyn = yesql.dynamic.AsyncDynamicQueryLib(posts)
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
    )
    persisted = await posts.create(model=post)
    found = await dyn.select(title=persisted.title)
    print(f"Dynamically selected posts: {found}")
    q = dyn.table.select(dyn.table.id).where(dyn.table.title == persisted.title)
    id = await dyn.execute(q, rtype="val")
    print(f"Dynamically for Post ID with {post.title=}: {id=}")
    await posts.delete(id=persisted.id)


if __name__ == "__main__":
    import asyncio
    import os

    os.environ["database_url"] = "blog.db"
    asyncio.run(run())
    asyncio.run(dynamic())
