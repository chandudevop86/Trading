from __future__ import annotations

from patch_validator.config import ValidatorConfig
from patch_validator.models import PatchFile, PatchIntent


TYPE_HINTS: list[tuple[str, tuple[str, ...]]] = [
    ("security", ("auth/", "security", "guard", "validation", "permission", "secret")),
    ("infra", (".github/workflows/", "infra/", "Dockerfile", "docker-compose", "alembic", "migration")),
    ("config", (".env", ".yaml", ".yml", ".json", "requirements", "settings", "config")),
    ("feature", ("routes/", "service.py", "strategy", "feature")),
    ("refactor", ("refactor", "rename", "cleanup")),
    ("bugfix", ("fix", "bug", "hotfix")),
]


def detect_patch_intent(changed_files: list[PatchFile], config: ValidatorConfig) -> PatchIntent:
    changed_paths = [item.path for item in changed_files]
    patch_types: set[str] = set()
    for path in changed_paths:
        lowered = path.lower()
        for patch_type, hints in TYPE_HINTS:
            if any(hint.lower() in lowered for hint in hints):
                patch_types.add(patch_type)
    if not patch_types:
        patch_types.add("refactor")
    if "bugfix" in patch_types and any("hotfix" in path.lower() for path in changed_paths):
        patch_types.add("hotfix")
    total_added = sum(item.added_line_count for item in changed_files)
    total_removed = sum(item.removed_line_count for item in changed_files)
    oversized = total_added >= config.oversized_patch_added_lines or len(changed_files) >= config.oversized_patch_file_count
    return PatchIntent(
        patch_types=sorted(patch_types),
        mixed_concerns=len(patch_types) > 1,
        oversized=oversized,
        changed_files=changed_paths,
        total_added_lines=total_added,
        total_removed_lines=total_removed,
    )
