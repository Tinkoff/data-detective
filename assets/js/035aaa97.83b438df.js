"use strict";(self.webpackChunkmg_af_docusaurus=self.webpackChunkmg_af_docusaurus||[]).push([[1286],{3905:function(e,t,r){r.d(t,{Zo:function(){return f},kt:function(){return d}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),l=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},f=function(e){var t=l(e.components);return n.createElement(c.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,f=s(e,["components","mdxType","originalType","parentName"]),u=l(r),d=a,m=u["".concat(c,".").concat(d)]||u[d]||p[d]||o;return r?n.createElement(m,i(i({ref:t},f),{},{components:r})):n.createElement(m,i({ref:t},f))}));function d(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=u;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:a,i[1]=s;for(var l=2;l<o;l++)i[l]=r[l];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}u.displayName="MDXCreateElement"},9086:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return s},contentTitle:function(){return c},metadata:function(){return l},toc:function(){return f},default:function(){return u}});var n=r(7462),a=r(3366),o=(r(7294),r(3905)),i=["components"],s={sidebar_label:"py_transform",title:"operators.transformers.py_transform"},c=void 0,l={unversionedId:"data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform",id:"data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform",isDocsHomePage:!1,title:"operators.transformers.py_transform",description:"PyTransform Objects",source:"@site/docs/data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform.md",sourceDirName:"data-detective-airflow/data_detective_airflow_api_reference/operators/transformers",slug:"/data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/py_transform.md",tags:[],version:"current",frontMatter:{sidebar_label:"py_transform",title:"operators.transformers.py_transform"},sidebar:"tutorialSidebar",previous:{title:"pg_sql",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/operators/transformers/pg_sql"},next:{title:"assertions",permalink:"/data-detective/docs/data-detective-airflow/data_detective_airflow_api_reference/test_utilities/assertions"}},f=[{value:"PyTransform Objects",id:"pytransform-objects",children:[{value:"execute",id:"execute",children:[],level:4}],level:2}],p={toc:f};function u(e){var t=e.components,r=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"pytransform-objects"},"PyTransform Objects"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"class PyTransform(TBaseOperator)\n")),(0,o.kt)("p",null,"Perform in-memory conversion"),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"source"),": List of sources"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"transformer"),": Handler (transformer) function"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"op_kwargs"),": Additional params for callable"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"kwargs"),": Additional params for TBaseOperator")),(0,o.kt)("h4",{id:"execute"},"execute"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"def execute(context: dict) -> None\n")),(0,o.kt)("p",null,"Only in-memory mode is supported\nSources are unpacked before entering the function.\nIf there are no sources, the function will not receive them in an empty list.\nThis can be avoided by using the following signature: def transformer(context, *sources):"),(0,o.kt)("p",null,"@param context: Execution context"))}u.isMDXComponent=!0}}]);