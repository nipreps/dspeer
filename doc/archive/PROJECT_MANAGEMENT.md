# Project Management - Peer-to-Peer (P2P) Dataset Federation

**Vincent Cordola** | ISC-ID-26-6 | HES-SO Valais-Wallis x McGill Montreal  
**Supervisors**: Prof. Oscar Esteban | Prof. Jean-Baptiste Poline | Dr. Nikhil Bhagwat  
**Period**: May 11, 2026 - August 12, 2026

This document provides an overview of the project. It includes the project plan, potential project risks, meeting schedules, and a work journal for daily tracking of the project.

This document will evolve as the project progresses, and the initial information was taken directly from the document: [Donnée du travail de bachelor](https://isc-hei.github.io/isc-typst-tb-descriptions/)

## Table of Contents
1. [Planning](#1-Planning)
2. [Risk Register](#2-risk-register)
3. [Reviews and Meetings](#3-reviews-and-meetings)
4. [Work Journal](#4-work-journal)

## 1. Planning

### 1.1 Deliverables
- **Milestone:** Tasks included in the PDF: [Donnée du travail de bachelor](https://isc-hei.github.io/isc-typst-tb-descriptions/)
- **Task:** Tasks created during the project to provide a clearer overview of the various steps to be completed

| Type | Deliverable | Target date | Status |
|---|-------------|-------------|--------|
| W1-W2 | **1. Onboarding & Technical Design** | 11.05 - 24.05 | Done |
| **Milestone** | Rust & iroh crash-course completed | 24.05 | Done |
| **Milestone** | Dataset layout conventions defined | 24.05 | Done |
| Task | Report setup, GitHub page, planning | 24.05 | Done |
| Task | logical topology (to get an overview of the project, first draft) | 24.05 | Done |
| W3-W6 | **2. Core P2P sharing prototype** | 25.05 - 21.06 | Done |
| **Milestone** | Rust MVP: (i) join an ad-hoc iroh network, (ii) advertise Parquet files, and (iii) fetch Parquet files from peers. | 05.06 | Done |
| Task | Setting up the Rust project | 31.05 | Done |
| Task | Join an ad hoc iroh network | 31.05 | Done |
| Task | Advertise and fetch Parquet files from peers | 05.06 | Done |
| Task | End-to-end test on two terminals (same machine) | 05.06 | Done |
| Task | Listen and fetch in parallel | 05.06 | Done |
| **Milestone** | Validation on a Docker Compose network of 3–5 containers. | 21.06 | Done |
| Task | Write the Dockerfile | 21.06 | Done |
| Task | Write the docker-compose.yml | 21.06 | Done |
| Task | Manual ticket exchange | 21.06 | Done |
| Task | End-to-end validation | 21.06 | Done |
| W7-W10 | **3. Python/Jupyter integration and connectivity validation** | 22.06 - 19.07 | Done |
| **Milestone** | Python client layer: a unified Jupyter interface ensuring required files are present locally via on-demand retrieval. | 19.07 | Done |
| Task | Complete the system architecture document with the rest of the project | 19.07 | Done |
| Task | Add iroh-gossip to the Rust node + generate a JSON manifest listing local files (tickets) and propagate it to peers | 19.07 | Done |
| Task | Add Axum HTTP server to the Rust node | 19.07 | Done |
| Task | Implement `P2PClient`, thin HTTP wrapper around the Rust API | 19.07 | Done |
| Task | Implement `P2PDataset`, cache management and on-demand fetch | 19.07 | Done |
| **Milestone** | Produce a first working notebook that discovers the network dataset, loads it into standard dataframe tooling, and runs queries spanning multiple peers. | 19.07 | Upcoming |
| Task | Implement p2p.load(filename), fetch + return pandas Dataframe via DuckDB | 19.07 | Done |
| Task | Implement p2p.query, DuckDB query across all cached Parquet files | 19.07 | Done |
| Task | Write demo notebooks | 19.07 | Upcoming |
| Task | End-to-end validation VM | 19.07 | Upcoming |
| **Milestone** | Migrate the testbed to virtual machines with separate network stacks to exercise NAT traversal and relay fallback for the first time. | 19.07 | Upcoming |
| Task | Set up 2 VirtualBox VMs with separate networks | 19.07 | Done |
| Task | Deploy and run the Rust node on both VMs | 19.07 | Done |
| Task | Validate NAT traversal and relay fallback (iroh handles it, validate via logs) | 19.07 | Upcoming |
| W11-W13 | **4. Robustness and evaluation in a 2–5 machine testbed** | 20.07 - 09.08 | Upcoming |
| **Milestone** | Test behavior under realistic conditions: peers joining/leaving, partial availability, varying bandwidth. | 09.08 | Upcoming |
| **Milestone** | Improve reliability (resume partial transfers, cache behavior, error handling) and document known limitations. | 09.08 | Upcoming |
| **Milestone** | Optional (if feasible): evaluate byte-range or partial reads to reduce transfers for larger Parquet artifacts. | 09.08 | Upcoming |
| W14 | **5. Packaging, documentation, and final demonstration** | 10.08 - 12.08 | Upcoming |
| **Milestone** | Finalize the notebook as the primary demonstration artifact. | 12.08 | Upcoming |
| **Milestone** | Produce setup scripts/instructions for reproducing the demo on 2–5 machines | 12.08 | Upcoming |
| **Milestone** | Write a short evaluation report summarizing results, constraints, and recommended next steps for downstream integration work. | 12.08 | Upcoming |
| **Milestone** | **Final code submitted** | **17/07/2026 12:00** | Upcoming |
| **Milestone** | **Final report submitted** | **12/08/2026 12:00** | Upcoming |
| **Milestone** | **Executive summary** | **25/08/2026** | Upcoming |
| **Milestone** | **Poster** | **25/08/2026** | Upcoming |



### 1.2 Key Milestones

| Milestone | Date |
|-----------|------|
| Bachelor thesis start | May 11, 2026 |
| Midterm presentation | June 3, 2026 |
| Oral defence | Weeks of August 17 - 25, 2026 |
| Final code submission | August 25, 2026 - 12:00 |
| Executive summary | August 25, 2026 |
| Final report submission | August 25, 2026 - 12:00 |
| Poster | August 25, 2026 |
| Poster exhibition | August 28, 2026 - HEI |
| Pitch challenge | August 31, 2026 - Monthey |

## 2. Risk Register
| Risk | Probability | Impact | Mitigation measures |
|------|-------------|--------|---------------------|
| Unstable iroh API / breaking changes | Medium | High | Pin dependency versions early, use only well-documented primitives (blobs, gossip) |
| Multi-machine testbed setup difficulties (heterogeneous network) | Medium | Medium | Phase 2 on Docker first, migrate to VMs in phase 3 only after Docker validation |
| Rust + Python integration learning curve | High | Medium | Keep Rust component minimal (thin node agent), focus effort on Python layer (PyArrow/DuckDB) |
| Parquet footer-first read pattern incompatible with iroh-blobs range requests | Medium | Low | Treated as optional, fallback = full file download into local cache |


## 3. Reviews and Meetings 
### Meeting - 01/06/2026

**Participants:** Prof. Oscar Esteban - Vincent Cordola     
**Type:** Overview of the project's progress  
**Duration:** 30 min

**Discussion points**
- Presentation of the planning and the project's progress
- Current progress on the Rust component and potential additional features
- Potential inclusion of DuckDB to the project (mention in the project pdf)

**Decisions made**
- Overall, the project is progressing very well
- Stick to the features listed in the PDF, don't get sidetracked by extra features. These could be added once the "main" project is finished
- It remains to be seen whether DuckDB can be easily integrated and wheter that would be useful

---

### Meeting - 02/06/2026

**Participants:** Vincent Cordola - Origami lab     
**Type:** Presentation project overall
**Duration:** 20 min

**Discussion points**
- General information about the project / bachelor thesis
- More details about the p2p.
- Speaking about the potential implementation, problems that may arise

**Decisions made**
- _

**media**
- [Introduction p2p projet](doc/media/bthesis_proj_intro.pdf)

---

### Meeting - 04/06/2026

**Participants:** Vincent Cordola - Dr. Sebastian Urchs - Dr. Nikhil Bhagwat    
**Type:** Project overview, discussion of potential implementation options for existing projects    
**Duration:** 60 min

**Discussion points**
- Project planning and Milestones
- What will be possible to do with the current project (data accessible from the notebook...)? How will it be shared, and who will have access to that?
- How might this apply to projects like Neurobagel? How could we implement this with more sensitive data? Is it possible to have different access levels for different files?
- Implementation in another project: How can I share only part of a dataset rather than all the files in a node?

**Decisions made**
- As part of the PoC
    - Potentially install DucDB to better query the file later
    - The API (between the jupyeter and our data) will certainly allow us to select only a subset of the data from our Jupyter notebook.
    - Data segmentation will likely not be done in Iroh part (so each peer will have all the data), which will make the data accessible on each node / more avaibility.
- Following the PoC / implement in other projects
    - Thanks to iroh's flexibility, this project can be adapted to different environments, but it will still require a lot of adjustment
    - A particular version of Jupyter will likely be best suited for a specific type of data or project. But that remains to be seen once the project is complete.


<!-- Reviews and Meetings template
### Meeting - [Date]

**Participants:** Vincent Cordola - x   
**Type:**   
**Duration:** _ min

**Discussion points**
- _

**Decisions made**
- _
-->

## 4. Work Journal

### 11/05/2026 - Report setup, GitHub page, planning

**Work done:**
- Reviewed the criteria for the bachelor's thesis using the information provided on ISC Learn.
- Created the project repository and also created the page for the report (template ISC bthesis on Typst).
- Initial review of MRIQC API to get a first idea of how to define the dataset layout convetions.

**Decisions / Observations / Blockers:**
- Create the project management file for this project is important for better monitoring

**Next steps:**
- Create the project management

---

### 12/05/2026 - Report setup, GitHub page, planning

**Work done:**
- Creation of the first version for project management markdown
- Research on MRIQC to subsequently establish dataset layout conventions
- Research on the overall project structure to subsentquently create an initial logical topology and gain a better understanding of each project.

**Decisions / Observations / Blockers:**

**Next steps:**
- Do the Dataset layout conventions
- Do the logical topology

---

### 13/05/2026

**Work done:**
- Research on tools for designing the dataset layout
    - [MRIQC](https://mriqc.readthedocs.io/en/latest/reports/group.html)
    - [DuckDB Parquet support](https://duckdb.org/docs/current/data/parquet/overview)
    - [pyarrow write table API, parameters,schema...](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)
    - [iroh_blobs](https://docs.rs/iroh-blobs/latest/iroh_blobs/)
    - [Parquet file anatomy](https://towardsdatascience.com/anatomy-of-a-parquet-file/)

**Decisions / Observations / Blockers:**
- Starting tomorrow, I'll have to write something "concrete," otherwise I risk spreading myself too thin. I'll try to come up with a first draft, even if it's minimal.

**Next steps:**
- Minimal version of Dataset layout convetions
- Minimal version of logical topology

---

### 14/05/2026

**Work done:**
- Start the minimal version of dataset layout
- Do some research about iroh, DuckDB
- Learning Rust


**Decisions / Observations / Blockers:**
- I'll need your approval of the dataset layout once it's finished

**Next steps:**
- Continue learning Rust
- Moving forward with the dataset layout
- Start the logical topology if possible

---

### 15/05/2026

**Work done:**
- Progress on the layout convetions dataset
- Learning about iroh to see how blobs work

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish the first draft for the dataset layout conventions
- Start logical topology
---

## Weekly Summary 11/05 - 17/05

**Abstract**
During this first week, i began laying the groundwork for the project. It was a bit of a struggle to get started and stay focused after moving to a new country. However, i was still able to make a good start on the planning, which will help me with the project's ongoing progress.

**Next week**
- I should finish the dataset layout document as well as the one on topology.
- Schedule weekly meetings and the interim presentation in early June.
- Keep learning Rust and iroh so I'll be ready to start implementation next week.

---

### 18/05/2026 - Public holiday (Journée nationale des Patriotes)

**Work done:**
- Preparing presentation of my project for the lab
- Rust learning
**Decisions / Observations / Blockers:**

**Next steps:**

---

### 19/05/2026

**Work done:**
- Converstation with Dr. Nikhil Bhagwat about QC studio
- Modification of the data layout convention document

**Decisions / Observations / Blockers:**

**Next steps:**
- Finalize the first draft of the data layout convetion
- Getting started with logical topology

---

### 20/05/2026

**Work done:**
- First draft for the dataset layout conventions
- Start of the document describing the project architecture
**Decisions / Observations / Blockers:**
- There will be updates in the future for the dataset layout conventions, this provides a starting point.

**Next steps:**
- Continue and finish the first draft for the architecture
- Continue with the Rust courses and possibly start focusing more on the early stages of development for the iroh component.
- Define how the development environment will be structured

---

### 21/05/2026

**Work done:**
- Progress on the architecture document
- Iroh documentation

**Decisions / Observations / Blockers:**
- I should be careful not to spend too much time reading the documentation, it’s better for me to start the implementation and learn as I go rather than just reading the documentation.

**Next steps:**
- Finish first draft architecture document
- Start implementation

---

### 22/05/2026

**Work done:**
- Finish first draft of architecture
- First search for the Rust MVP (List the main tasks in the planning)
- Start of the installation guide for the Rust MVP
- Tokio documentation

**Decisions / Observations / Blockers:**

**Next steps:**
- Continue the rust-mvp


## Weekly Summary 18/05 - 24/05

**Abstract**
During this second week, I did a lot of research on the technologies I would be using for this project. I also now have a good overall picture of how I want to structure the project.

**Next week**
If possible, I'll try to finish the Rust MVP (non Docker version). With all the examples I can find online, I should be able to quickly exchange PArquet files between two terminals. If all goes well, I'll be able to package this into Docker containers the following week.

---
### 25/05/2026

**Work done:**
- New search on iroh regarding endpoints
- Review of the general documentation

**Decisions / Observations / Blockers:**
- I need to provide some initial results regarding iroh, I don't need to go into that much detail in the documentation

**Next steps:**
- Continue join an ad hoc iroh network

---

### 26/05/2026

**Work done:**
- Good progress on Join an ad hoc iroh network 
- The dependencies should work fine for this part

**Decisions / Observations / Blockers:**
- Rust with Iroh is even more complicated than what I've seen before, I'm groing to have to keep it as simple as possible for this to work.
- The examples provided on the iroh computer GitHub repository are very helpful.

**Next steps:**
- Finish the Join an ad hoc iroh network part
- Make sure to document this section properly
- Start Advertise and fetch Parquet files from peers

---

### 27/05/2026

**Work done:**
- Finish code for join an ad-hoc iroh network
- Progress in the documentation
- Research on the "network" functionality of this component (QUIC, NAT, hole punching, ...)

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish the installation guide on join an ad-hoc iroh network
- Start the section on Parquet file exchange

---

### 28/05/2026

**Work done:**
- Progress on the installation guide
- Documentation about the manifest

**Decisions / Observations / Blockers:**
- For the manifest, I'll be able to use / modify from what's been done for data lakehouse (for example, with Apache Iceberg).
- I'm currently using iroh relays for hole punching between endpoints. I shouldn't have any network issues for the rest of the projet. We'll see how it goes.

**Next steps:**
- Finish installation guide for join an ad-hoc iroh network
- Start advertise / fetch Parquet from peers

---

### 29/05/2026

**Work done:**
- Finish Join an ad hoc iroh network
- Documentation for the advertise and fetch parquet file
- Start parquet implementation.

**Decisions / Observations / Blockers:**
- Since I'm currently on track with my planning, I'm taking the time to focus a bit more on how I'll manage my files. To do this, I plan to use the Serde framwork to manage my manifests. This will provide a robust validation of my files right from the start and make any future changes easier.

**Next steps:**
- Continue advertise Parquet files, and fetch Parquet files from peers.


## Weekly Summary 25/05 - 31/05

**Abstract**
THe first use/implementation of iroh was a success. I was able to connect right away, and as a bonus, any firewall or other issues are already resolved. NAT hole punching is already handled by the iroh servers. This allows us to establish connections beyond the LAN without having to set up port forwarding or route our data through relay servers. The iroh relay server is there solely for the handshake.

**Next week**
- Finish advertise Parquet files, and fetch Parquet files from peers.
- Start test in docker environment 

---

### 01/06/2026

**Work done:**
- Code for advertise and fetch parquet file
- Test on two terminals

**Decisions / Observations / Blockers:**
- Focus on the points specifically mentioned in the PDF and don't start working on a manifest file

**Next steps:**
- Complete the documentation for advertise and fetch parquet file
- Start Docker if possible

---

### 02/06/2026

**Work done:**
- Presentation of my project at the lab [Introduction p2p projet](doc/media/bthesis_proj_intro.pdf)
- Continue the installation guide for listen-blobs and fetch-blobs

**Decisions / Observations / Blockers:**
- Provide a clear description of the system architecture (how the NAT hole punching works...)

**Next steps:**
- Finish installation guide and update system architecture with this part
- Start Docker

---

### 03/06/2026

**Work done:**
- Finish iroh setup guide
- New version of peer, to make fetch and listen work together

**Decisions / Observations / Blockers:**
- Iroh setup is a first version, add iroh gossip and file naming would be very useful

**Next steps:**
- Update system architecture for iroh part (nat hole punching explanation...)
- Start docker part

---

### 04/06/2026

**Work done:**
- Make progress on the documentation for system_architecture
- Reading documentation for the Docker part
- Presentation/discussion/brainstorming on the project with Dr. Sebastian Urchs and Dr. Nikhil Bhagwat    

**Decisions / Observations / Blockers:**

**Next steps:**
- Start implementation of the Docker part

---

### 05/06/2026

**Work done:**
- Search for the Docker section
- Confirm the Iroh section

**Decisions / Observations / Blockers:**

**Next steps:**
- Start Docker implementation

## Weekly Summary 01/06 - 07/06

**Abstract**
- The Iroh component was completed with a very solid MVP already in place. Finally, the two functions were integrated into a Tokio runtime, which will be better suited for the rest of the project (a node can either listen or fetch). The addition of direct reading in the terminal will also allow us to fetch data continuously without blocking the program.
- I was also able to hold two presentations/brainstorming sessions with the Origami Lab team regarding my project and its potential future implementation with other projects, which allowed me to step back from the project and ask myself questions I wouldn’t have necessarily thought of at first.


**Next week**
- Finish 2. Core P2P sharing prototype (docker part)
- Start 3. Python/Jupyter integration and connectivity validation

---

### 08/06/2026

**Work done:**
- Folder for the docker testbed
- Start of the Dockerfile
- Documentation about Rust with docker to create the simplest possible version

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish Dockerfile and docker-compose

---

### 09/06/2026

**Work done:**
- Finish docker part
- Start defining the tasks for this section: 3. Python/Jupyter integration and connectivity validation

**Decisions / Observations / Blockers:**
- The next section is really important for the project, so I'll need to keep it simple to get somenthing functional up and running first.

**Next steps:**
- Finish defining task for: Python client layer: a unified Jupyter interface ensuring required files are present locally via on-demand retrieval.

---

### 10/06/2026

**Work done:**
- Research on how to implement the API... (Axum)

**Decisions / Observations / Blockers:**
- It's hard to find a balance between keeping things simple enough to validate the PoC, but also finding something that can evolve in the future so we can continue / upgrade the project.
- The goal, therefore, is to find the right balance in terms of complexity.

**Next steps:**
- Clearly defin the etechnical sutrcture of this part (API etc..) to get a better overview
- Defining the tasks for this milestone. 

---

### 11/06/2026

**Work done:**
- Finish defining the tasks for the next three milestones

**Decisions / Observations / Blockers:**
- It's hard to define the right balance between difficulty and future upgrade

**Next steps:**
- Finish system architecture for the new blocs
- Start iroh-gossip implementation

---

### 12/06/2026

**Work done:**
- Complete the "brainstorming" phase for the various components of the projet

**Decisions / Observations / Blockers:**
- The goal is to keep each part clearly separate so that if I need to modify or improve a section, I don't have to change the entire project, which is especially important for the future. And i also need to make sure everything runs smoothly without the final user even noticing.

**Next steps:**
- Start iroh-gossip implementation


## Weekly Summary 08/06 - 14/06

**Abstract**
It's been a week of brainstorming, and a lot of things I hadn't considered are now coming into play for the next phase of the project. I'm glad I got a head start on the previous part, it allowed me to step back and take an overview of the project, and better defin what I'll be doing over the next few weeks.

**Next week**
- Start implementation, iroh, http axum, python api, client

---

### 15/06/2026

**Work done:**
- Continue with system architecture mardown

**Decisions / Observations / Blockers:**
- There were still a few thing to consider (local storage, keeping the data in folder, or migrating them to a database...)
- I think it's still better to clearly define what needs to be done to avoid problems in the futur and get a better sense of the available options.

**Next steps:**
- Finish system architecture
- Start iroh-gossip implementation

---

### 16/06/2026

**Work done:**
- Finish system architecture mardown update
- Start iroh-gossip

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish iroh-gossip / first manifest

---

### 17/06/2026

**Work done:**
- Progress on iroh-gossip (broadcast, bootstreap peers)
- Search on Serde for the small manifest

**Decisions / Observations / Blockers:**
- I'll try to finish this this week, there are a lot of things to take into account. I need to keep it simple, and in worst case, I'll move to the next tasks

**Next steps:**
- Finish iroh-gossip part

---

### 18/06/2026

**Work done:**
- Progress iroh-gossip (manifest part)

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish iroh-gossip

---

### 19/06/2026

**Work done:**
- Finish iroh-gossip implementation

**Decisions / Observations / Blockers:**
- It works very well, there may be some adjustments to make to the manifest sharing, but overall it works very well

**Next steps:**
- Finish documentation about iroh-gossip / manifest


## Weekly Summary 15/06 - 21/06

**Abstract**
This week has again been heavily focused on the overall architecture, with the implementation of iroh gossip, which is working very well.

**Next week**
- Finish iroh-gossip documentation
- Add Axum HTTP server to the Rust node

---

### 22/06/2026

**Work done:**
- Start of documentation for the iroh gossip / manifest sharing section

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish the documentation
- Start Axum http server

---

### 23/06/2026

**Work done:**
- Almost done for the iroh gossip / manifest documentation

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish this documentation
- Start Axum http server

---

### 24/06/2026 (Fête nationale du Québec)

**Work done:**
- Finish gossip setup guide

**Decisions / Observations / Blockers:**

**Next steps:**
- Start http Axum

---

### 25/06/2026

**Work done:**
- Start of the implementation of the HTTP API

**Decisions / Observations / Blockers:**
- Create a new folder that will serve as the final project folder

**Next steps:**
- Progress on axum http server

---

### 26/06/2026

**Work done:**
- Finish Axum http API

**Decisions / Observations / Blockers:**
- Since the next task doesn't directly involve the Rust network, I don't need to adapt everything to Docker and i can test it on a single peer.

**Next steps:**
- Finish documentation about the API implementation

## Weekly Summary 22/06 - 28/06

**Abstract**
- Iroh and the API are working very well
- Everything works very well only with the Cargo build in the terminal, and the three requests are already working fine, which will be a big plus for continuing the project.

**Next week**
- Start python part (python cache, P2PClient, P2PDataset)

---
### 29/06/2026

**Work done:**
- Finish documentation about the API implementation
- Brainstorm ideas on what to implement before the end of the project / what's needed for the PoC. And separate "nice to have" feature for the future
- Research how to get started with the Python portion

**Decisions / Observations / Blockers:**

**Next steps:**
- Start Python client.py

---

### 30/06/2026

**Work done:**
- Create P2PError and P2PClient

**Decisions / Observations / Blockers:**

**Next steps:**
- Test and documentation about this part

### 01/07/2026 (fête du Canada)

**Work done:**

**Decisions / Observations / Blockers:**

**Next steps:**

---

### 02/07/2026

**Work done:**
- Finish documentation and test for P2PClient

**Decisions / Observations / Blockers:**

**Next steps:**
- Start index.json and P2PDataset

---

### 03/07/2026

**Work done:**
- Progress for dataset.py

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish dataset.py
- Start VM / Jupyter


## Weekly Summary 29/06 - 05/07

**Abstract**
- Implementation of the "bridge" between Python and Rust. HTTP requests to `Axum` via Python `requests` library work very well.

**Next week**
- Finish this part, then start the implementation using Jupyter Notebooks and a VM

### 06/07/2026

**Work done:**
- Progress for dataset.py docuementation

**Decisions / Observations / Blockers:**
- Possible schedule adjustment, as the code delivery date will be moved from the end of week 14 to week 10.

**Next steps:**
- Finish dataset.py documentation, p2p.load and p2p.query

---

### 07/07/2026

**Work done:**
- Finish dataset.py
- new planing for next week

**Decisions / Observations / Blockers:**

**Next steps:**
- End dataset.py documentation
- requirements.txt, data folder, manifest update

---

### 08/07/2026

**Work done:**
- Finish dataset.py documentation
- Start Installation guide mardown

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish installation guide
- Test on 2 VM

---

### 09/07/2026

**Work done:**
- add parquet metadata in the manifest
- installation documentation
- Installation of Debian VM for the next test
**Decisions / Observations / Blockers:**

**Next steps:**
- Finish dataset.files() to make it easier to read
- test on 2 VM

---

### 10/07/2026

**Work done:**
- Refresh manifest automatically
- pre-release of the project
- Change columns view for the notebook
- update installation.md
- Test installation on 2 VM

**Decisions / Observations / Blockers:**

**Next steps:**
- Test NAT traversal and relay fallback
- Start readme and update all the documentation

## Weekly Summary 06/07 - 12/07

**Abstract**
- Finish the V1 for the project (code and infra part)
- Validation on 2 peer from different continent
- Small fix in the code

**Next week**
- Finish documentation (mardown in the repos)
- Release the reposs

---

### 13/07/2026

**Work done:**
- Refactor the repos
- Start evaluation documentation
- Define last step before the github release

**Decisions / Observations / Blockers:**

**Next steps:**
- Finish evaluation markdown
- Create costum function example for taxi parquet, finish demo notebooks

## Weekly Summary 13/07 - 19/07

**Abstract**

**Next week**



<!-- journal tempalte

### DD/MM/2026

**Work done:**

**Decisions / Observations / Blockers:**

**Next steps:**

-->

