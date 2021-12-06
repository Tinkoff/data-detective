"use strict";(self.webpackChunkdata_detective_docusaurus=self.webpackChunkdata_detective_docusaurus||[]).push([[9386],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return c}});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function p(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},l=Object.keys(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var o=r.createContext({}),s=function(e){var t=r.useContext(o),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},u=function(e){var t=s(e.components);return r.createElement(o.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},k=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,l=e.originalType,o=e.parentName,u=p(e,["components","mdxType","originalType","parentName"]),k=s(n),c=a,m=k["".concat(o,".").concat(c)]||k[c]||d[c]||l;return n?r.createElement(m,i(i({ref:t},u),{},{components:n})):r.createElement(m,i({ref:t},u))}));function c(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=n.length,i=new Array(l);i[0]=k;var p={};for(var o in t)hasOwnProperty.call(t,o)&&(p[o]=t[o]);p.originalType=e,p.mdxType="string"==typeof e?e:a,i[1]=p;for(var s=2;s<l;s++)i[s]=n[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}k.displayName="MDXCreateElement"},9311:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return p},contentTitle:function(){return o},metadata:function(){return s},toc:function(){return u},default:function(){return k}});var r=n(7462),a=n(3366),l=(n(7294),n(3905)),i=["components"],p={sidebar_label:"sftp_work",title:"dag_generator.works.sftp_work"},o=void 0,s={unversionedId:"data_detective_airflow_api_reference/dag_generator/works/sftp_work",id:"data_detective_airflow_api_reference/dag_generator/works/sftp_work",isDocsHomePage:!1,title:"dag_generator.works.sftp_work",description:"TSFTPWork",source:"@site/docs/data_detective_airflow_api_reference/dag_generator/works/sftp_work.md",sourceDirName:"data_detective_airflow_api_reference/dag_generator/works",slug:"/data_detective_airflow_api_reference/dag_generator/works/sftp_work",permalink:"/data-detective/docs/data_detective_airflow_api_reference/dag_generator/works/sftp_work",editUrl:"https://github.com/TinkoffCreditSystems/data-detective/edit/master/tools/doc-site/docs/data_detective_airflow_api_reference/dag_generator/works/sftp_work.md",tags:[],version:"current",frontMatter:{sidebar_label:"sftp_work",title:"dag_generator.works.sftp_work"},sidebar:"tutorialSidebar",previous:{title:"s3_work",permalink:"/data-detective/docs/data_detective_airflow_api_reference/dag_generator/works/s3_work"},next:{title:"tbaseoperator",permalink:"/data-detective/docs/data_detective_airflow_api_reference/operators/tbaseoperator"}},u=[{value:"SFTPWork Objects",id:"sftpwork-objects",children:[{value:"exists",id:"exists",children:[],level:4},{value:"execute",id:"execute",children:[],level:4},{value:"mkdir",id:"mkdir",children:[],level:4},{value:"rmdir",id:"rmdir",children:[],level:4},{value:"write",id:"write",children:[],level:4},{value:"write_bytes",id:"write_bytes",children:[],level:4},{value:"read",id:"read",children:[],level:4},{value:"read_bytes",id:"read_bytes",children:[],level:4},{value:"is_dir",id:"is_dir",children:[],level:4},{value:"is_file",id:"is_file",children:[],level:4},{value:"get_size",id:"get_size",children:[],level:4}],level:2}],d={toc:u};function k(e){var t=e.components,n=(0,a.Z)(e,i);return(0,l.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"TSFTPWork"),(0,l.kt)("p",null,"The module contains the TSFTPWork class, which implements the logic of working with\nfile Work on a remote machine via the SFTP protocol"),(0,l.kt)("h2",{id:"sftpwork-objects"},"SFTPWork Objects"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"class SFTPWork(BaseFileWork)\n")),(0,l.kt)("h4",{id:"exists"},"exists"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef exists(path: str, sftp_client: Optional[SFTPClient] = None) -> bool\n")),(0,l.kt)("p",null,"Check if the path exists on the remote machine"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("h4",{id:"execute"},"execute"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def execute(command: str, sync: bool = False) -> int\n")),(0,l.kt)("p",null,"Run a command on a remote machine"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"command"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sync"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("h4",{id:"mkdir"},"mkdir"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def mkdir(path: str, mode: int = 0o777)\n")),(0,l.kt)("p",null,"Recursive directory creation"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": Path to the directory"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"mode"),": Rights granted to directories")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Raises"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"IOError"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"OSError"),": ")),(0,l.kt)("h4",{id:"rmdir"},"rmdir"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"def rmdir(path: str, recursive: bool = False)\n")),(0,l.kt)("p",null,"Delete by path on a remote machine"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": Path to the fs object"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"recursive"),": Delete nested objects")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Raises"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"IOError"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"OSError"),": ")),(0,l.kt)("h4",{id:"write"},"write"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef write(path: str, text: str, sftp_client: SFTPClient = None)\n")),(0,l.kt)("p",null,"Writing text to a file by the path"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"text"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("h4",{id:"write_bytes"},"write","_","bytes"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef write_bytes(path: str, bts: bytes, sftp_client: SFTPClient = None)\n")),(0,l.kt)("p",null,"Writing bytes to a file by the path"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"bts"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("h4",{id:"read"},"read"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef read(path: str, sftp_client: SFTPClient = None) -> str\n")),(0,l.kt)("p",null,"Reading text by the path"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("h4",{id:"read_bytes"},"read","_","bytes"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef read_bytes(path: str, sftp_client: SFTPClient = None) -> bytes\n")),(0,l.kt)("p",null,"Reading bytes from a file by the path"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("h4",{id:"is_dir"},"is","_","dir"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef is_dir(path: str, sftp_client: SFTPClient = None) -> bool\n")),(0,l.kt)("p",null,"Check if the specified path is a directory on the remote machine"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Raises"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"FileNotFoundError"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("h4",{id:"is_file"},"is","_","file"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef is_file(path: str, sftp_client: SFTPClient = None) -> bool\n")),(0,l.kt)("p",null,"Check if the specified path is a file on the remote machine"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": "),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": ")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Raises"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"FileNotFoundError"),": ")),(0,l.kt)("h4",{id:"get_size"},"get","_","size"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-python"},"@provide_sftp\ndef get_size(path: str, sftp_client: SFTPClient = None) -> str\n")),(0,l.kt)("p",null,"Get the size of the object in the sftp work.\nIf the object is missing, it returns -1"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Arguments"),":"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"path"),": Object name"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sftp_client"),": Connection")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Returns"),":"),(0,l.kt)("p",null,"Rounded object size"))}k.isMDXComponent=!0}}]);