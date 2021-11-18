"use strict";(self.webpackChunkmg_af_docusaurus=self.webpackChunkmg_af_docusaurus||[]).push([[779],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return d}});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),m=p(n),d=o,f=m["".concat(s,".").concat(d)]||m[d]||u[d]||a;return n?r.createElement(f,i(i({ref:t},c),{},{components:n})):r.createElement(f,i({ref:t},c))}));function d(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var p=2;p<a;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},1455:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return p},toc:function(){return c},default:function(){return m}});var r=n(7462),o=n(3366),a=(n(7294),n(3905)),i=["components"],l={sidebar_position:5},s="Running the dev environment using docker-compose",p={unversionedId:"mg-airflow/development",id:"mg-airflow/development",isDocsHomePage:!1,title:"Running the dev environment using docker-compose",description:"Deploying the development environment",source:"@site/docs/mg-airflow/development.md",sourceDirName:"mg-airflow",slug:"/mg-airflow/development",permalink:"/docs/mg-airflow/development",editUrl:"https://github.com/facebook/docusaurus/edit/main/website/docs/mg-airflow/development.md",tags:[],version:"current",sidebarPosition:5,frontMatter:{sidebar_position:5},sidebar:"tutorialSidebar",previous:{title:"Creating a new operator",permalink:"/docs/mg-airflow/operators"},next:{title:"DAG testing",permalink:"/docs/mg-airflow/testing"}},c=[{value:"Setup in PyCharm Professional",id:"setup-in-pycharm-professional",children:[],level:3},{value:"Working in a dev environment",id:"working-in-a-dev-environment",children:[],level:3},{value:"Notes",id:"notes",children:[],level:2}],u={toc:c};function m(e){var t=e.components,n=(0,o.Z)(e,i);return(0,a.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"running-the-dev-environment-using-docker-compose"},"Running the dev environment using docker-compose"),(0,a.kt)("p",null,"Deploying the development environment"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"cd mg-airflow\ncp .env.example .env\nrandstr=`shuf -zer -n32  {A..Z} {a..z} {0..9} | tr -d '\\0'`\necho \"SECRET_KEY=${randstr}\" >> .env\ndocker-compose up -d\n")),(0,a.kt)("p",null,"It is possible to check mg-airflow with ",(0,a.kt)("a",{parentName:"p",href:"https://airflow.apache.org/docs/stable/executor/celery.html"},"Celery Executor"),"\nas follows:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"docker-compose -f docker-compose.CeleryExecutor.yml up -d\n")),(0,a.kt)("p",null,"This services will be launched:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"redis - queue storage"),(0,a.kt)("li",{parentName:"ul"},"webserver - portal"),(0,a.kt)("li",{parentName:"ul"},"scheduler - scheduler that runs tasks"),(0,a.kt)("li",{parentName:"ul"},"flower - view current queues"),(0,a.kt)("li",{parentName:"ul"},"worker - a service that runs tasks, scalable, 2 instances")),(0,a.kt)("p",null,"To run autotests, run the command"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests\n")),(0,a.kt)("p",null,"For all Executor, postgres is used as a metadata repository (metadb).\nIn a production environment, it is advisable to use the Celery Executor option."),(0,a.kt)("h3",{id:"setup-in-pycharm-professional"},"Setup in PyCharm Professional"),(0,a.kt)("p",null,"Configuring SSH Python interpreter in PyCharm"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Preferences (CMD + ,) > Project Settings > Project Interpreter")),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},'Click on the gear icon next to the "Project Interpreter" dropdown > Add')),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},'Select "SSH Interpreter" > Host: localhost'),(0,a.kt)("p",{parentName:"li"},"Port: 9922, "),(0,a.kt)("p",{parentName:"li"},"Username: airflow. "),(0,a.kt)("p",{parentName:"li"},"Password: see ",(0,a.kt)("inlineCode",{parentName:"p"},"./Dockerfile:15")),(0,a.kt)("p",{parentName:"li"},"Interpreter: /usr/local/bin/python3, "),(0,a.kt)("p",{parentName:"li"},"Sync folders: Project Root -> /usr/local/airflow"),(0,a.kt)("p",{parentName:"li"},'Disable "Automatically upload..."')),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Confirm the changes and wait for PyCharm to update the indexes")),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"To run tests go to PyCharm -> Edit Configurations -> Environment variables: "))),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"AIRFLOW_HOME=/usr/local/airflow;PYTHON_PATH=/usr/local/bin/python/:/usr/local/airflow:/usr/local/airflow/dags;AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@metadb:5432/airflow\n")),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"In order not to specify Environment variables every time, they should be added to Templates -> Python tests -> pytest (\u0432 Edit Configurations)"),(0,a.kt)("li",{parentName:"ol"},"Setting up py test. To do this, in the Pycharm settings, select\nTools->Python Integrated Tools->Testing->Default test runner = pytest.\nAfter that, it is possible to run tests by right-clicking on the directory ",(0,a.kt)("inlineCode",{parentName:"li"},"tests")," or the desired test file.")),(0,a.kt)("h3",{id:"working-in-a-dev-environment"},"Working in a dev environment"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Airflow UI is available at http://localhost:8080/.\nLogin/password to log in - airflow /airflow.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"To connect to a dev container with airflow, select this options below in PyCharm: ",(0,a.kt)("inlineCode",{parentName:"p"},"Tools > Start SSH Session > Remote Python ...")," or ",(0,a.kt)("inlineCode",{parentName:"p"},"docker-compose exec app bash")," (app - service name in docker-compose.yml, bash - command for executing)"))),(0,a.kt)("h2",{id:"notes"},"Notes"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"At the time of publication, Docker Compose in the command line can be called with a command without a hyphen ",(0,a.kt)("inlineCode",{parentName:"li"},"docker compose ..."),".\nThe documentation will contain examples with ",(0,a.kt)("inlineCode",{parentName:"li"},"docker-compose ..."),".")))}m.isMDXComponent=!0}}]);