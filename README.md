Solar Energy Data Pipeline
Context
Atlantica Sustainable Infrastructure is a global renewable energy company with operations across multiple countries. This project focuses on their Chilean solar PV portfolio, where the technical team faced a recurring problem: every time new generation data needed to be loaded, the previous data would get overwritten or lost. Everything was being managed in Excel, and at a certain scale, Excel simply stops being a reliable tool.
I was brought in to solve it. I had no prior access to any of the company's systems, no documentation about the plants, and very little context about how the Chilean electricity sector works. That changed quickly.
The Problem
The goal was to build a single, reliable table where anyone at the company could query generation data, marginal costs, meter readings, plant names, and timestamps, all in one place, without fear of losing anything when new data arrives.
The data came from two main sources published by the Coordinador Eléctrico Nacional: real generation data and marginal costs, both covering January 2025 through January 2026. The immediate challenge was that these two datasets had no obvious column to join them on. Date and time alone were not enough because multiple plants share the same timestamp, and a naive join would have produced thousands of duplicate records. The solution required a mapping table provided by the technical team, which linked each plant to its corresponding meter ID and grid connection point. From there, the join became possible and the duplicates were resolved.
Technical Challenges
Not everything went smoothly, and that is worth documenting.
The meter data files use a European number format, with commas as decimal separators and periods as thousands separators. A standard pandas read would silently convert values like 2.488,87 into NaN. This had to be handled explicitly before any conversion to MWh.
One plant, Jama, had its injection and withdrawal columns completely inverted compared to every other plant in the dataset. What the file labeled as injection was actually withdrawal, and vice versa. This was not documented anywhere and only became apparent after cross-referencing the values with expected generation patterns. A conditional transformation was applied specifically for that plant.
The historical meter portal from the Coordinador organizes data by substation letter, packaged as ZIP files, each containing dozens of CSVs covering the entire national grid. I only needed 8 specific meters out of hundreds. The portal has no public API and uses a dynamic JavaScript interface that cannot be scraped with simple HTTP requests. Playwright was used to control a real browser, navigate the tree structure programmatically, download only the relevant ZIP files, filter the records in memory, and discard everything else. Downloading manually would have taken days.
What I Built
A MySQL relational database structured around a central table called historicos, which consolidates generation in MWh, marginal costs in both USD and CLP, meter injection readings, plant names, meter IDs, and timestamps at 15-minute resolution. New data can be appended at any time without overwriting existing records.
The priority logic for generation data works as follows: if a meter reading exists for a given timestamp, that value is used. If not, the coordinator's generation figure is used as fallback. This ensures the most accurate source is always prioritized automatically, without manual intervention.
Why It Matters
Before this pipeline existed, loading new data meant risking the loss of everything already stored. Queries that required crossing multiple Excel files now run in seconds. The company has a single source of truth for generation data across all plants, with a clear audit trail of where each value came from.
This is an ongoing project. SCADA integration and irradiance data are next.
Tech Stack
Python, pandas, MySQL, SQLAlchemy, Playwright, DBeaver
Notes
Credentials are managed via a .env file not included in this repository. Plant-level data belongs to Atlantica and is not shared here. Only the code and schema are public.
Voluntary independent project, January 2025 to present.
