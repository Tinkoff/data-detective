"use strict";(self.webpackChunkdata_detective_docusaurus=self.webpackChunkdata_detective_docusaurus||[]).push([[3265],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return f}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),p=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},u=function(e){var t=p(e.components);return n.createElement(c.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),d=p(r),f=a,m=d["".concat(c,".").concat(f)]||d[f]||s[f]||o;return r?n.createElement(m,l(l({ref:t},u),{},{components:r})):n.createElement(m,l({ref:t},u))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,l=new Array(o);l[0]=d;var i={};for(var c in t)hasOwnProperty.call(t,c)&&(i[c]=t[c]);i.originalType=e,i.mdxType="string"==typeof e?e:a,l[1]=i;for(var p=2;p<o;p++)l[p]=r[p];return n.createElement.apply(null,l)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},2642:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return i},contentTitle:function(){return c},metadata:function(){return p},toc:function(){return u},default:function(){return d}});var n=r(7462),a=r(3366),o=(r(7294),r(3905)),l=["components"],i={sidebar_label:"tbaseoperator",title:"operators.tbaseoperator"},c=void 0,p={unversionedId:"data_detective_airflow_api_reference/operators/tbaseoperator",id:"data_detective_airflow_api_reference/operators/tbaseoperator",isDocsHomePage:!1,title:"operators.tbaseoperator",description:"TBaseOperator Objects",source:"@site/docs/data_detective_airflow_api_reference/operators/tbaseoperator.md",sourceDirName:"data_detective_airflow_api_reference/operators",slug:"/data_detective_airflow_api_reference/operators/tbaseoperator",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/tbaseoperator",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data_detective_airflow_api_reference/operators/tbaseoperator.md",tags:[],version:"current",frontMatter:{sidebar_label:"tbaseoperator",title:"operators.tbaseoperator"},sidebar:"tutorialSidebar",previous:{title:"sftp_work",permalink:"/data-detective/docs/data_detective_airflow_api_reference/dag_generator/works/sftp_work"},next:{title:"db_dump",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/extractors/db_dump"}},u=[{value:"TBaseOperator Objects",id:"tbaseoperator-objects",children:[{value:"get_conn_id",id:"get_conn_id",children:[],level:4},{value:"execute",id:"execute",children:[],level:4},{value:"pre_execute",id:"pre_execute",children:[],level:4},{value:"post_execute",id:"post_execute",children:[],level:4},{value:"generate_context",id:"generate_context",children:[],level:4},{value:"read_result",id:"read_result",children:[],level:4}],level:2}],s={toc:u};function d(e){var t=e.components,r=(0,a.Z)(e,l);return(0,o.kt)("wrapper",(0,n.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"tbaseoperator-objects"},"TBaseOperator Objects"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"class TBaseOperator(BaseOperator,  ABC)\n")),(0,o.kt)("p",null,"Base operator.\nAll other operators need to inherit from this one."),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"description"),": Task description"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"conn_id"),": Optional connection for connecting to a particular source"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"work_conn_id"),": Optional connection work for the operator (taken from the dag by default)"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"result_type"),": Optional result type/format (taken from dag by default)"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"work_type"),": Optional work type/format (taken from dag by default)")),(0,o.kt)("h4",{id:"get_conn_id"},"get","_","conn","_","id"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"def get_conn_id() -> str\n")),(0,o.kt)("p",null,"Get conn_id from the task or from default DAG settings"),(0,o.kt)("h4",{id:"execute"},"execute"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"@abstractmethod\ndef execute(context)\n")),(0,o.kt)("p",null,"Execute the operator"),(0,o.kt)("h4",{id:"pre_execute"},"pre","_","execute"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"@prepare_lineage\ndef pre_execute(context: dict = None)\n")),(0,o.kt)("p",null,"The method is called before calling self.execute()"),(0,o.kt)("h4",{id:"post_execute"},"post","_","execute"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"@apply_lineage\ndef post_execute(context: dict = None, result=None)\n")),(0,o.kt)("p",null,"The method is called immediately after calling self.execute()"),(0,o.kt)("h4",{id:"generate_context"},"generate","_","context"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"def generate_context(execution_date=datetime.now()) -> Dict[str, Any]\n")),(0,o.kt)("p",null,"Generate a context for an execution_date of any kind."),(0,o.kt)("h4",{id:"read_result"},"read","_","result"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"def read_result(context)\n")),(0,o.kt)("p",null,"Read the result. Used in tests.\nIn statements that do not write to work, you need to redefine."),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"context"),": Execution context")),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Returns"),":"),(0,o.kt)("p",null,"Dataset"))}d.isMDXComponent=!0}}]);