"use strict";(self.webpackChunkmg_af_docusaurus=self.webpackChunkmg_af_docusaurus||[]).push([[303],{3905:function(e,r,t){t.d(r,{Zo:function(){return s},kt:function(){return m}});var n=t(7294);function o(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function a(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?a(Object(t),!0).forEach((function(r){o(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function l(e,r){if(null==e)return{};var t,n,o=function(e,r){if(null==e)return{};var t,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||(o[t]=e[t]);return o}(e,r);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var c=n.createContext({}),u=function(e){var r=n.useContext(c),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},s=function(e){var r=u(e.components);return n.createElement(c.Provider,{value:r},e.children)},f={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},p=n.forwardRef((function(e,r){var t=e.components,o=e.mdxType,a=e.originalType,c=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),p=u(t),m=o,g=p["".concat(c,".").concat(m)]||p[m]||f[m]||a;return t?n.createElement(g,i(i({ref:r},s),{},{components:t})):n.createElement(g,i({ref:r},s))}));function m(e,r){var t=arguments,o=r&&r.mdxType;if("string"==typeof e||o){var a=t.length,i=new Array(a);i[0]=p;var l={};for(var c in r)hasOwnProperty.call(r,c)&&(l[c]=r[c]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var u=2;u<a;u++)i[u]=t[u];return n.createElement.apply(null,i)}return n.createElement.apply(null,t)}p.displayName="MDXCreateElement"},9688:function(e,r,t){t.r(r),t.d(r,{frontMatter:function(){return l},contentTitle:function(){return c},metadata:function(){return u},toc:function(){return s},default:function(){return p}});var n=t(7462),o=t(3366),a=(t(7294),t(3905)),i=["components"],l={},c="How to release mg-airflow",u={unversionedId:"mg-airflow/release",id:"mg-airflow/release",isDocsHomePage:!1,title:"How to release mg-airflow",description:'Create a release with tag like "mg-airflow/*" in repo.',source:"@site/docs/mg-airflow/release.md",sourceDirName:"mg-airflow",slug:"/mg-airflow/release",permalink:"/docs/mg-airflow/release",editUrl:"https://github.com/facebook/docusaurus/edit/main/website/docs/mg-airflow/release.md",tags:[],version:"current",frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"Abstractions for datasets",permalink:"/docs/mg-airflow/datasets"}},s=[],f={toc:s};function p(e){var r=e.components,t=(0,o.Z)(e,i);return(0,a.kt)("wrapper",(0,n.Z)({},f,t,{components:r,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"how-to-release-mg-airflow"},"How to release mg-airflow"),(0,a.kt)("p",null,'Create a release with tag like "mg-airflow/*" in repo.\nAction "Publish mg-airflow" will be triggered.'),(0,a.kt)("p",null,"Package version is extracted from ",(0,a.kt)("inlineCode",{parentName:"p"},"pyproject.toml"),"."))}p.isMDXComponent=!0}}]);