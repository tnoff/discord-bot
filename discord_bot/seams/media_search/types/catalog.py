from pydantic import BaseModel, Field


class CatalogItem(BaseModel):
    '''Individual Item'''
    search_string: str
    title: str | None = None

class CatalogResponse(BaseModel):
    '''Response from 3rd Party Catalog'''
    items: list[CatalogItem] = Field(default_factory=list)
    collection_name: str | None = None
