from dataclasses import dataclass, field


@dataclass
class CatalogItem:
    '''Individual Item'''
    search_string: str
    title: str = None

@dataclass
class CatalogResponse:
    '''Response from 3rd Party Catalog'''
    items: list[CatalogItem] = field(default_factory=list)
    collection_name: str = None
