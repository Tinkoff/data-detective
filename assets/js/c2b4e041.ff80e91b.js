"use strict";(self.webpackChunkdata_detective_docusaurus=self.webpackChunkdata_detective_docusaurus||[]).push([[9885],{3905:function(e,t,r){r.d(t,{Zo:function(){return d},kt:function(){return _}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var p=n.createContext({}),s=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},d=function(e){var t=s(e.components);return n.createElement(p.Provider,{value:t},e.children)},l={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,p=e.parentName,d=c(e,["components","mdxType","originalType","parentName"]),u=s(r),_=a,f=u["".concat(p,".").concat(_)]||u[_]||l[_]||i;return r?n.createElement(f,o(o({ref:t},d),{},{components:r})):n.createElement(f,o({ref:t},d))}));function _(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=u;var c={};for(var p in t)hasOwnProperty.call(t,p)&&(c[p]=t[p]);c.originalType=e,c.mdxType="string"==typeof e?e:a,o[1]=c;for(var s=2;s<i;s++)o[s]=r[s];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}u.displayName="MDXCreateElement"},4525:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return c},contentTitle:function(){return p},metadata:function(){return s},toc:function(){return d},default:function(){return u}});var n=r(7462),a=r(3366),i=(r(7294),r(3905)),o=["components"],c={sidebar_label:"pg_scd1_df_update_insert",title:"operators.sinks.pg_scd1_df_update_insert"},p=void 0,s={unversionedId:"data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert",id:"data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert",isDocsHomePage:!1,title:"operators.sinks.pg_scd1_df_update_insert",description:"PgSCD1DFUpdateInsert Objects",source:"@site/docs/data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert.md",sourceDirName:"data_detective_airflow_api_reference/operators/sinks",slug:"/data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data_detective_airflow_api_reference/operators/sinks/pg_scd1_df_update_insert.md",tags:[],version:"current",frontMatter:{sidebar_label:"pg_scd1_df_update_insert",title:"operators.sinks.pg_scd1_df_update_insert"},sidebar:"tutorialSidebar",previous:{title:"pg_scd1",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/sinks/pg_scd1"},next:{title:"pg_single_target_loader",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/sinks/pg_single_target_loader"}},d=[{value:"PgSCD1DFUpdateInsert Objects",id:"pgscd1dfupdateinsert-objects",children:[],level:2}],l={toc:d};function u(e){var t=e.components,r=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,n.Z)({},l,r,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"pgscd1dfupdateinsert-objects"},"PgSCD1DFUpdateInsert Objects"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"class PgSCD1DFUpdateInsert(PgLoader)\n")),(0,i.kt)("p",null,"Update the target table by SCD 1 by diff_change_operation"),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"source"),": Source"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"conn_id"),": Connection id"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"table_name"),": Table name for update"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"key"),": The key by which update. Avoid NULL for the key."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"diff_change_oper"),": Field with the flag of the operation to be applied to the record D,U,I"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"chunk_row_number"),": The number of rows in the chunk to load into the database and apply to the table")))}u.isMDXComponent=!0}}]);