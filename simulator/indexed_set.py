"""Set with O(1) add, remove and uniform random choice."""

import random


class IndexedSet:
    def __init__(self, items: list[str] | None = None) -> None:
        self._items: list[str] = []
        self._pos: dict[str, int] = {}
        for item in items or []:
            self.add(item)

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, item: str) -> bool:
        return item in self._pos

    def add(self, item: str) -> None:
        self._pos[item] = len(self._items)
        self._items.append(item)

    def remove(self, item: str) -> None:
        i = self._pos.pop(item)
        last = self._items.pop()
        if i < len(self._items):
            self._items[i] = last
            self._pos[last] = i

    def choice(self, rng: random.Random) -> str:
        return self._items[rng.randrange(len(self._items))]

    def pop_random(self, rng: random.Random) -> str:
        item = self.choice(rng)
        self.remove(item)
        return item
