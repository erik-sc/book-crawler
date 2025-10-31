from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Book:
    id: str
    title: str
    authors: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    self_link: Optional[str] = None
    thumbnail_url: Optional[str] = None

    def to_csv_row(self) -> str:
        safe = lambda x: x.replace(",", " ") if isinstance(x, str) else x
        authors_str = "|".join(self.authors)
        categories_str = "|".join(self.categories)

        return f"{safe(self.id)},{safe(self.title)},{authors_str},{categories_str},{self.self_link or ''},{self.thumbnail_url or ''}\n"

    @classmethod
    def from_csv(cls, row: list[str]) -> Optional["Book"]:
        if len(row) < 6:
            return None

        return cls(
            id=row[0],
            title=row[1],
            authors=row[2].split("|") if row[2] else [],
            categories=row[3].split("|") if row[3] else [],
            self_link=row[4] or None,
            thumbnail_url=row[5] or None,
        )
    
    @classmethod
    def from_api(cls, id: str, item: dict) -> "Book":
        volume = item.get("volumeInfo", {})
        self_link = item.get("selfLink")
        return cls(
            id=id,
            title=volume.get("title") or "Sem título",
            authors=volume.get("authors") or [],
            categories=volume.get("categories") or [],
            self_link=self_link,
        )
