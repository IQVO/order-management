import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import HomepageFeatures from '@site/src/components/HomepageFeatures';
import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns and terminology (WMS/WES/WCS,
      chaotic storage, CloudEvents, RFC 7807, hexagonal architecture). Not a
      production system. Not affiliated with, endorsed by, or representative
      of Amazon or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · upstream front door · Generic/Supporting subdomain
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          Before this context existed, "an order" was just an unowned,
          unvalidated string reinvented three different ways. Order
          Management makes <code>Order</code> and <code>OrderLine</code>{' '}
          first-class aggregates, and becomes the Customer that calls
          inventory-storage's published REST API, announces released work to
          wes-work-planning over Kafka, and reads product-master's
          classifications from a local copy.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/overview">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference">
            API Reference
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/adr">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Order Management bounded context: order intake, allocation, promise-date calculation, release and cancellation.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              The order-level <code>Status</code> is always{' '}
              <strong>derived</strong> from the line statuses — never a
              redundant field that can drift out of sync.
            </blockquote>
            <p className={styles.invariantCaption}>
              The design decision this entire bounded context turns on.{' '}
              <Link to="/docs/business-context/domain-vision">
                Why it reads that way →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}
