"use strict";(self.webpackChunkmg_af_docusaurus=self.webpackChunkmg_af_docusaurus||[]).push([[388],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return p}});var o=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,o)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,o,n=function(e,t){if(null==e)return{};var r,o,n={},a=Object.keys(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=o.createContext({}),l=function(e){var t=o.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=l(e.components);return o.createElement(c.Provider,{value:t},e.children)},f={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},d=o.forwardRef((function(e,t){var r=e.components,n=e.mdxType,a=e.originalType,c=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),d=l(r),p=n,m=d["".concat(c,".").concat(p)]||d[p]||f[p]||a;return r?o.createElement(m,i(i({ref:t},u),{},{components:r})):o.createElement(m,i({ref:t},u))}));function p(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var a=r.length,i=new Array(a);i[0]=d;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:n,i[1]=s;for(var l=2;l<a;l++)i[l]=r[l];return o.createElement.apply(null,i)}return o.createElement.apply(null,r)}d.displayName="MDXCreateElement"},1966:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return s},contentTitle:function(){return c},metadata:function(){return l},toc:function(){return u},default:function(){return d}});var o=r(7462),n=r(3366),a=(r(7294),r(3905)),i=["components"],s={sidebar_position:8},c="Comparison with other frameworks",l={unversionedId:"data-detective-airflow/comparison",id:"data-detective-airflow/comparison",isDocsHomePage:!1,title:"Comparison with other frameworks",description:"DAG configurators for Airflow",source:"@site/docs/data-detective-airflow/comparison.md",sourceDirName:"data-detective-airflow",slug:"/data-detective-airflow/comparison",permalink:"/data-detective/docs/data-detective-airflow/comparison",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data-detective-airflow/comparison.md",tags:[],version:"current",sidebarPosition:8,frontMatter:{sidebar_position:8},sidebar:"tutorialSidebar",previous:{title:"Production",permalink:"/data-detective/docs/data-detective-airflow/production"},next:{title:"Abstractions for datasets",permalink:"/data-detective/docs/data-detective-airflow/datasets"}},u=[{value:"DAG configurators for Airflow",id:"dag-configurators-for-airflow",children:[],level:2},{value:"DAG orchestrators",id:"dag-orchestrators",children:[],level:2}],f={toc:u};function d(e){var t=e.components,r=(0,n.Z)(e,i);return(0,a.kt)("wrapper",(0,o.Z)({},f,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"comparison-with-other-frameworks"},"Comparison with other frameworks"),(0,a.kt)("h2",{id:"dag-configurators-for-airflow"},"DAG configurators for Airflow"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://github.com/chriscardillo/gusty"},"gusty")," gusty it works with different input formats, supports TaskGroup."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://github.com/ajbosco/dag-factory"},"dag-factory")," dag-factory creates DAGs from YAML. It is also supported by TaskGroup.")),(0,a.kt)("h2",{id:"dag-orchestrators"},"DAG orchestrators"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://github.com/dagster-io/dagster"},"dagster")," This framework has a lot in common with Apache Airflow.\nThe scheduler and UI are divided into different modules. Work with s3 resources and local files is available.\nDagster implements a concept with work, creation and cleaning upon completion of work. There is also a quick scheduler here."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://argoproj.github.io/argo-workflows/"},"Argo Workflows")," This solution works on Go. Containers are launched in Kubernetes.\nIt is convenient to use because of the isolation of virtual environments. However, there is a difficulty in implementing testing.\nIt is necessary to run pipelines on Go, in which datasets in python will be compared.")))}d.isMDXComponent=!0}}]);