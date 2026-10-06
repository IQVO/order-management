import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  to: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Order Intake',
    to: '/docs/ddd/use-cases',
    description: (
      <>
        <code>ReceiveOrder</code> validates every line (non-empty SKU,
        positive quantity) and mints a real <code>OrderId</code> — closing the
        gap where an order was just an unowned string threaded through three
        other services.
      </>
    ),
  },
  {
    title: 'Fail-Closed Allocation',
    to: '/docs/ddd/aggregate-design-canvas',
    description: (
      <>
        The allocation pass inside <code>ReceiveOrder</code> and{' '}
        <code>RetryAllocation</code> reserves stock per line via
        inventory-storage. Only a <code>409</code> is treated as a
        backorder — a transport failure or 5xx fails the pass rather than
        silently marking a line backordered.
      </>
    ),
  },
  {
    title: 'Ship-Complete by Default',
    to: '/docs/business-context/domain-vision',
    description: (
      <>
        <code>allowPartialShipment</code> defaults to <code>false</code>: any
        backordered line holds the whole order back from release until{' '}
        <code>RetryAllocation</code> clears it — the only sanctioned route
        back to <code>Allocated</code>.
      </>
    ),
  },
  {
    title: 'Cancellation Boundary at Release',
    to: '/docs/ddd/aggregate-design-canvas',
    description: (
      <>
        <code>CancelOrder</code> is legal only while no line has reached{' '}
        <code>Released</code>. A legal cancellation revokes every allocated
        line's reservation before the aggregate is mutated at all.
      </>
    ),
  },
];

function Feature({title, to, description}: FeatureItem) {
  return (
    <div className={clsx('col col--3')}>
      <Link to={to} className={styles.featureCard}>
        <Heading as="h3" className={styles.featureTitle}>
          {title}
        </Heading>
        <p className={styles.featureBody}>{description}</p>
      </Link>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
