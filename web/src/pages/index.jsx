import React from "react";
import Layout from "../components/layout";
import SEO from "../components/seo";
import FastaForm from "../components/search";

const IndexPage = () => (
    <Layout>
      <SEO title="kAAmer search"/>
      <FastaForm/>
    </Layout>
);

export default IndexPage;
