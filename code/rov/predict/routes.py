"""Typed candidates, selection results, and per-prefix routing state."""

from dataclasses import dataclass
from dataclasses import field


@dataclass(frozen=True, slots=True)
class Candidate:
    peer: str
    next_hop: str
    origin_as: int
    segments: tuple
    origin: int
    med: int
    rov: str = field(compare=False)
    unsupported_attributes: bool = field(default=False, compare=False)


@dataclass(frozen=True, slots=True)
class BestPath:
    path: Candidate | None = None
    peers: tuple[str, ...] = ()

    # Distinguish a definite result, including no path, from unresolved selection.
    @property
    def known(self):
        return not self.peers


@dataclass(slots=True)
class PrefixRoutes:
    candidates: dict[str, Candidate] = field(default_factory=dict)
    best: BestPath = field(default_factory=BestPath)
