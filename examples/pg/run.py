from db import client, model


async def run():
    posts = client.Posts("postgres://postgres:@localhost:5432/blog?sslmode=disable")
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    persisted = await posts.create(model=post)
    print(f"Created a post: {persisted!r}")
    persisted.title = "Maybe It's Not So Great After All"
    updated = await posts.update(model=post, id=post.id)
    print(f"Updated a post: {updated!r}")
    deleted = await posts.delete(id=post.id)
    print(f"Deleted a post: {deleted!r}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
