import { useEffect, useState } from "react";
import { Sparkles } from "lucide-react";
import type { ModelOptionsResult } from "@hermes/shared";
import { api, fetchJSON, getManagementProfile } from "@/lib/api";
import { Badge } from "@nous-research/ui/ui/components/badge";
import { Button } from "@nous-research/ui/ui/components/button";
import { Label } from "@nous-research/ui/ui/components/label";
import { Select, SelectOption } from "@nous-research/ui/ui/components/select";
import { Spinner } from "@nous-research/ui/ui/components/spinner";
import { errorMessage } from "@/lib/api-error";

/* One fallback chain entry: {provider, model} plus the pointer fields the
 * runtime resolves (base_url, key_env) — never a plaintext api_key. */
interface FallbackEntry {
  provider: string;
  model: string;
  base_url?: string;
  key_env?: string;
}

interface FallbackAutofillResponse {
  ok: boolean;
  provider: string;
  requested: number;
  added: FallbackEntry[];
  excluded: { entry: FallbackEntry; reason: string }[];
  chain: FallbackEntry[];
}

/* "model (via provider)  [base_url]" — same wording as the CLI's
 * `hermes fallback list` renderer (fallback_cmd._format_entry). */
function formatEntry(entry: FallbackEntry): string {
  const base = entry.base_url ? `  [${entry.base_url}]` : "";
  return `${entry.model}  (via ${entry.provider})${base}`;
}

/**
 * One-click fallback auto-fill for the config page's model section: pick a
 * provider, take the first 5 of ITS models (short lists fill completely) and
 * append them to `fallback_providers` — hand-added entries are kept, the
 * primary deployment and duplicates are skipped server-side.
 *
 * Writes go through POST /api/model/fallback/auto-fill (the same write path
 * `hermes fallback add` uses), so the chain never round-trips through the
 * generic schema form here — no save touches it. The profile is sent in the
 * POST body (the route reads body.profile) rather than relying on the
 * api-request-scope query-suffix mechanism.
 */
export function FallbackAutofillField({ profile }: { profile?: string }) {
  const [providers, setProviders] = useState<
    { slug: string; name: string; models: string[] }[]
  >([]);
  const [selected, setSelected] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<FallbackAutofillResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getModelOptions(profile ? { profile } : undefined)
      .then((resp: ModelOptionsResult) => {
        if (cancelled) return;
        const rows = (resp.providers ?? [])
          .filter((p) => p.slug && (p.models?.length ?? 0) > 0)
          .map((p) => ({
            slug: p.slug,
            name: String(p.name || p.slug),
            models: (p.models ?? []).map(String),
          }));
        setProviders(rows);
        setSelected((prev) => prev || rows[0]?.slug || "");
      })
      .catch(() => {
        /* provider list stays empty; the select shows "no providers" */
      });
    return () => {
      cancelled = true;
    };
  }, [profile]);

  const autoFill = async () => {
    if (!selected || busy) return;
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      const body = {
        provider: selected,
        top_n: 5,
        profile: profile || getManagementProfile(),
      };
      const resp = await fetchJSON<FallbackAutofillResponse>(
        "/api/model/fallback/auto-fill",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      setResult(resp);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  const sameDeployment = result
    ? result.excluded.filter((x) => x.reason === "same_deployment").length
    : 0;
  const duplicates = result
    ? result.excluded.filter((x) => x.reason === "duplicate").length
    : 0;

  return (
    <div className="grid gap-2 py-1">
      <Label className="text-sm">Fallback auto-fill</Label>
      <p className="text-xs text-text-secondary">
        Append the first 5 models of one provider to the fallback chain.
        Hand-added entries are kept; the primary model and duplicates are
        skipped.
      </p>
      <div className="flex flex-wrap items-center gap-2">
        <div className="min-w-0 flex-1 sm:max-w-xs">
          <Select
            value={selected}
            onValueChange={(v) => {
              setSelected(v);
              setResult(null);
              setError(null);
            }}
          >
            {providers.length === 0 ? (
              <SelectOption value="">no providers with models</SelectOption>
            ) : (
              providers.map((p) => (
                <SelectOption key={p.slug} value={p.slug}>
                  {p.name} ({p.models.length})
                </SelectOption>
              ))
            )}
          </Select>
        </div>
        <Button size="sm" onClick={autoFill} disabled={busy || !selected}>
          {busy ? (
            <Spinner className="h-3.5 w-3.5" />
          ) : (
            <Sparkles className="h-3.5 w-3.5" />
          )}
          {busy ? "Filling…" : "Auto-fill top 5"}
        </Button>
      </div>

      {error && (
        <p className="text-xs text-destructive">Auto-fill failed: {error}</p>
      )}

      {result && (
        <div className="grid gap-1 rounded-md border border-border bg-muted/20 px-3 py-2">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs font-semibold">
              Fallback chain ({result.chain.length})
            </span>
            <Badge tone="secondary" className="text-xs">
              +{result.added.length} added
            </Badge>
            {sameDeployment > 0 && (
              <Badge tone="secondary" className="text-xs">
                {sameDeployment} skipped (primary)
              </Badge>
            )}
            {duplicates > 0 && (
              <Badge tone="secondary" className="text-xs">
                {duplicates} duplicates skipped
              </Badge>
            )}
          </div>
          {result.chain.length === 0 ? (
            <p className="text-xs text-text-tertiary">Chain is empty.</p>
          ) : (
            <ol className="grid gap-0.5">
              {result.chain.map((entry, i) => (
                <li
                  key={`${entry.provider}:${entry.model}:${entry.base_url ?? ""}:${i}`}
                  className="truncate font-mono text-xs text-text-secondary"
                >
                  {i + 1}. {formatEntry(entry)}
                </li>
              ))}
            </ol>
          )}
          <p className="text-xs text-text-tertiary">
            Saved to config.yaml — the gateway picks the chain up on its next
            turn.
          </p>
        </div>
      )}
    </div>
  );
}
