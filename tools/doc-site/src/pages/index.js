import React from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Quickstart from '../components/main/Quickstart';
import { DataCatalogFeature } from '../components/main/DataCatalogFeature';
import { AboutLineage } from '../components/main/AboutLineage';
import { HomepageHeader } from '../components/main/HomepageHeader';
import { FlexibleSearch } from '../components/main/FlexibleSearch';
import { UiUx } from '../components/main/UiUx';

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`Welcome to Data Detective official documentation`}
      description="Description will go into a meta tag in <head />">
      <HomepageHeader />

      <DataCatalogFeature />
      <AboutLineage />
      <FlexibleSearch />
      <UiUx />
      <Quickstart />
    </Layout>
  );
}
