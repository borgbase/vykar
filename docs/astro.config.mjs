// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import mermaid from 'astro-mermaid';
import starlightLinksValidator from 'starlight-links-validator';

const site = 'https://vykar.borgbase.com';
const ogImage = `${site}/images/vykar-color.jpg`;

export default defineConfig({
	site,
	// Emit backup.html instead of backup/index.html to keep the existing extensionless URLs on Cloudflare Pages.
	build: { format: 'file' },
	integrations: [
		// Must come before starlight so it sees the mermaid code fences.
		mermaid({
			autoTheme: true,
			enableLog: false,
			mermaidConfig: {
				flowchart: { useMaxWidth: true, nodeSpacing: 18, rankSpacing: 24, padding: 4 },
			},
		}),
		starlight({
			title: 'Vykar Backup',
			description:
				'Fast, encrypted, deduplicated backups written in Rust, with a simple YAML config, desktop GUI and REST server.',
			logo: { src: './src/assets/logo-simple.svg' },
			favicon: '/favicon.svg',
			customCss: ['./src/styles/custom.css'],
			editLink: { baseUrl: 'https://github.com/borgbase/vykar/edit/main/docs/' },
			components: {
				SocialIcons: './src/components/SocialIcons.astro',
				Hero: './src/components/Hero.astro',
			},
			head: [
				{ tag: 'link', attrs: { rel: 'icon', href: '/favicon.png', type: 'image/png', sizes: '32x32' } },
				{ tag: 'meta', attrs: { property: 'og:image', content: ogImage } },
				{ tag: 'meta', attrs: { property: 'og:image:alt', content: 'Vykar Backup Logo' } },
				{ tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
				{ tag: 'meta', attrs: { name: 'twitter:image', content: ogImage } },
				{ tag: 'meta', attrs: { name: 'twitter:image:alt', content: 'Vykar Backup Logo' } },
				{
					tag: 'script',
					attrs: {
						defer: true,
						src: 'https://stats.snapdragon.cc/script.js',
						'data-website-id': 'ed5f57fc-0177-4b98-9b12-5b295b9faf83',
					},
				},
			],
			plugins: [starlightLinksValidator()],
			sidebar: [
				{ label: 'Home', link: '/' },
				'quickstart',
				{
					label: 'Usage',
					items: [
						'install',
						'init-setup',
						'backends',
						'backup',
						'restore',
						'daemon',
						'maintenance',
						'gui',
						'recipes',
					],
				},
				{ label: 'Server', items: ['server-setup'] },
				{ label: 'Reference', items: ['configuration', 'commands', 'json-output'] },
				{ label: 'Background', items: ['comparison', 'goals', 'roadmap'] },
				{
					label: 'Internals',
					collapsed: true,
					items: ['architecture', 'server-internals', 'testing'],
				},
			],
		}),
	],
});
