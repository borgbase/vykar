export const repo = 'borgbase/vykar';

async function fetchStars(): Promise<number | undefined> {
	const headers: Record<string, string> = { Accept: 'application/vnd.github+json' };
	if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
	try {
		const res = await fetch(`https://api.github.com/repos/${repo}`, {
			headers,
			signal: AbortSignal.timeout(5000),
		});
		if (!res.ok) return undefined;
		const { stargazers_count } = await res.json();
		return typeof stargazers_count === 'number' ? stargazers_count : undefined;
	} catch {
		return undefined;
	}
}

// Module-level so the API is hit once per build, not once per page.
export const stars = fetchStars();

export function formatStars(n: number): string {
	if (n < 1000) return String(n);
	return `${(n / 1000).toFixed(n >= 10000 ? 0 : 1).replace(/\.0$/, '')}k`;
}
