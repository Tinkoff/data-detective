import React from 'react';
import clsx from 'clsx';
import styles from '../../pages/index.module.css'
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import MainButton from './Button/MainButton';

export function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary main-section', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <div className={styles.buttons}>
          {/* <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/welcome">
            Welcome to Data Detective documenation!
          </Link> */}
          <MainButton text="Welcome to Data Detective documentation!" link="/docs/welcome"/>
        </div>
      </div>
    </header>
  );
}
