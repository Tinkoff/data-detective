import React from 'react';
import clsx from 'clsx';
import styles from '../../pages/index.module.css'
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary main-section', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <div className={styles.buttons}>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/intro">
            Welcome to MG documenation!
          </Link>
        </div>
      </div>
    </header>
  );
}