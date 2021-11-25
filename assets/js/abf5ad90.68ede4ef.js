"use strict";(self.webpackChunkmg_af_docusaurus=self.webpackChunkmg_af_docusaurus||[]).push([[4052],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return f}});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},l=Object.keys(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=a.createContext({}),d=function(e){var t=a.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=d(e.components);return a.createElement(c.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,l=e.originalType,c=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),p=d(r),f=n,k=p["".concat(c,".").concat(f)]||p[f]||s[f]||l;return r?a.createElement(k,i(i({ref:t},u),{},{components:r})):a.createElement(k,i({ref:t},u))}));function f(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=r.length,i=new Array(l);i[0]=p;var o={};for(var c in t)hasOwnProperty.call(t,c)&&(o[c]=t[c]);o.originalType=e,o.mdxType="string"==typeof e?e:n,i[1]=o;for(var d=2;d<l;d++)i[d]=r[d];return a.createElement.apply(null,i)}return a.createElement.apply(null,r)}p.displayName="MDXCreateElement"},3422:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return o},contentTitle:function(){return c},metadata:function(){return d},toc:function(){return u},default:function(){return p}});var a=r(7462),n=r(3366),l=(r(7294),r(3905)),i=["components"],o={sidebar_label:"pickle_result",title:"dag_generator.results.pickle_result"},c=void 0,d={unversionedId:"data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result",id:"data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result",isDocsHomePage:!1,title:"dag_generator.results.pickle_result",description:"PickleResult Objects",source:"@site/docs/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result.md",sourceDirName:"data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results",slug:"/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pickle_result.md",tags:[],version:"current",frontMatter:{sidebar_label:"pickle_result",title:"dag_generator.results.pickle_result"},sidebar:"tutorialSidebar",previous:{title:"pg_result",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/results/pg_result"},next:{title:"base_db_work",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/dag_generator/works/base_db_work"}},u=[{value:"PickleResult Objects",id:"pickleresult-objects",children:[{value:"write_df",id:"write_df",children:[],level:4},{value:"write",id:"write",children:[],level:4},{value:"read_df",id:"read_df",children:[],level:4},{value:"read",id:"read",children:[],level:4}],level:2}],s={toc:u};function p(e){var t=e.components,r=(0,n.Z)(e,i);return(0,l.kt)("wrapper",(0,a.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("h2",{id:"pickleresult-objects"},"PickleResult Objects"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"class PickleResult(BaseResult)\n")),(0,l.kt)("p",null,"Processes the result of a task stored in a bit sequence in a file"),(0,l.kt)("h4",{id:"write_df"},"write","_","df"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def write_df(obj: Any, context: dict)\n")),(0,l.kt)("p",null,"Write the Data Frame to the file storage.\nSends a call to write"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"obj"),": Dataset for writing"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"context"),": context")),(0,l.kt)("h4",{id:"write"},"write"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def write(obj: Any, context: dict)\n")),(0,l.kt)("p",null,"Write an object to file storage."),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"obj"),": Object for writing"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"context"),": context")),(0,l.kt)("h4",{id:"read_df"},"read","_","df"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def read_df(context: dict)\n")),(0,l.kt)("p",null,"Read DataFrame from file storage.\nSends a call to work.read"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"context"),": context")),(0,l.kt)("h4",{id:"read"},"read"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def read(context: dict) -> Any\n")),(0,l.kt)("p",null,"Read object from file storage."),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"context"),": context")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("p",null,"DataFrame"))}p.isMDXComponent=!0}}]);