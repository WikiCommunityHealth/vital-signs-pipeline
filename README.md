# Wikipedia Community Health Dashboards - Vital Signs Pipeline

This repository contains the data-processing pipeline powering the **Wikipedia Community Health Dashboards**, a system for computing and updating *Community Vital Signs*—a set of language-independent indicators that measure the health and sustainability of Wikipedia editor communities over time.

The pipeline is fully automated, open source, and designed to run on a monthly schedule, producing reproducible and comparable metrics across all Wikipedia language editions.

📊 **Live dashboards:** [https://vitalsigns.wmcloud.org](https://vitalsigns.wmcloud.org)

💾 **Data downloads:** [https://vitalsigns.wmcloud.org/data](https://vitalsigns.wmcloud.org/data)

📝 **Demo paper:** *Wikipedia Community Health Dashboards* 


## Overview

Wikipedia depends on hundreds of volunteer communities operating across more than 300 language editions. Understanding the long-term health of these communities requires metrics that are reproducible from public data, comparable across languages, consistent over time, and regularly updated.

This pipeline implements the computation of the **Community Vital Signs**, a framework originally introduced by Miquel-Ribé, Consonni, and Laniado (2022), and operationalized here at scale through a production-ready data workflow.

> Miquel-Ribé, M.; Consonni, C.; Laniado, D.
> _Community Vital Signs: Measuring Wikipedia Communities’ Sustainable Growth and Renewal._
> Sustainability 2022, 14, 4705. https://doi.org/10.3390/su14084705 

### Vital Signs
Metric                | Indicator              
--------------------- | -----------------------
 Retention            | Retention rate
 Stability            | Stability
 Balance              | Balance
 Special functions    | Technical editors
 Special functions    | Coordinators
 Admins               | Admins by year
 Admins               | Admins by lustrum
 Admins               | Admins by lustrum
 Global participation | Meta‑wiki participation
 Global participation | Primary language
 Activity             | Active users

 1. **Retention rate** – Percentage of new editors who edit at least once 60 days after their first edit.
 1. **Stability** – Number of active editors measured by the number of consecutive months they have been active.
 1. **Balance** – Number and percentage of very active editors by year and by generation (lustrum of the first edit).
 1. **Technical editors** – Number of very active editors in technical namespaces (editors performing more than 100 edits in one month in namespaces Mediawiki and Templates), broken down by year and by generation.
 1. **Coordinators** – Number of very active editors in coordination namespaces (editors performing more than 100 edits in one month in namespaces Wikipedia and Help), broken down by year and by generation.
 1. **Admins by year** – Number of administrators by year of flag grant and by generation.
 1. **Admins by lustrum** – Total number of active administrators by generation at the current month. This indicator appears twice in the original table and is therefore repeated here.
 1. **Meta‑wiki participation** – Ratio between the number of active editors on Meta‑wiki with a given primary language edition and the number of active editors in that Wikipedia language edition during the same month.
 1. **Primary language** – Distribution of the primary language edition of the editors contributing to a given language edition.
 1. **Active users** – Number of editors with 5 or more edits in a month.

## System Architecture

The pipeline is implemented as a containerized, orchestrated workflow. with the following components:

![Wikipedia Community Dashboards System Architecture](https://vitalsigns.wmcloud.org/assets/architecture.png)

The entire process—from dump ingestion to metric computation—is automated and designed for monthly execution.


## License

This project is released under the MIT license. See the `LICENSE.md` file for details. All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
