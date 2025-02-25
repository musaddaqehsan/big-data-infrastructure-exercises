from typing import Union

from fastapi import APIRouter, status

v0_router = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/v0",
    tags=["v0"],
)


@v0_router.get("/")
def hello_world() -> dict:
    """Typical Hello world example but with the API"""
    return {"Hello": "World"}


@v0_router.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None) -> dict:
    """Pass a variable in the URL and return query params (?param=1...)"""
    return {"item_id": item_id, "q": q}
