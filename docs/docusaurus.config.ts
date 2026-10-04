import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type * as OpenApiPlugin from 'docusaurus-plugin-openapi-docs';

const config: Config = {
  title: 'Order Management',
  tagline:
    'Order intake, allocation, promise-date calculation, and release — the missing upstream Open Host Service for the warehouse-systems fleet.',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
    faster: true,
  },

  url: 'https://iqvo.github.io',
  baseUrl: '/order-management/',

  organizationName: 'IQVO',
  projectName: 'order-management',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/IQVO/order-management/tree/main/docs/',
          docItemComponent: '@theme/ApiItem',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      'docusaurus-plugin-openapi-docs',
      {
        id: 'openapi',
        docsPluginId: 'classic',
        config: {
          order: {
            // The single source of truth: the same Spectral-linted spec the
            // service ships and CI gates on. Never hand-transcribed here.
            specPath: '../apis/openapi.yaml',
            outputDir: 'docs/api-reference/rest',
            sidebarOptions: {
              groupPathsBy: 'tag',
              categoryLinkSource: 'tag',
            },
            hideSendButton: true,
          } satisfies OpenApiPlugin.Options,
        },
      },
    ],
  ],

  themes: ['docusaurus-theme-openapi-docs', '@docusaurus/theme-mermaid'],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Order Management',
      logo: {
        alt: 'Order Management',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/api-reference/rest/order-management-api',
          label: 'API Reference',
          position: 'left',
        },
        {
          to: '/docs/adr',
          label: 'ADRs',
          position: 'left',
        },
        {
          href: 'https://github.com/IQVO/order-management',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Overview', to: '/docs/overview'},
            {label: 'Business Context', to: '/docs/business-context/domain-vision'},
            {label: 'Domain-Driven Design', to: '/docs/ddd/subdomain-classification'},
            {label: 'API Reference', to: '/docs/api-reference'},
          ],
        },
        {
          title: 'Ecosystem',
          items: [
            {label: 'Context map', to: '/docs/ecosystem/context-map'},
            {label: 'inventory-storage', href: 'https://github.com/IQVO/inventory-storage'},
            {label: 'wes-work-planning', href: 'https://github.com/IQVO/wes-work-planning'},
            {label: 'workforce-management', href: 'https://github.com/IQVO/workforce-management'},
            {label: 'fulfillment-execution', href: 'https://github.com/IQVO/fulfillment-execution'},
            {label: 'facility-layout', href: 'https://github.com/IQVO/facility-layout'},
          ],
        },
        {
          title: 'Source',
          items: [
            {label: 'GitHub repository', href: 'https://github.com/IQVO/order-management'},
            {label: 'OpenAPI spec', href: 'https://github.com/IQVO/order-management/blob/main/apis/openapi.yaml'},
          ],
        },
      ],
      copyright: `Order Management — a warehouse-systems bounded context. Built ${new Date().getFullYear()}.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'go', 'json', 'yaml', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
