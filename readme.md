# CONCEPT

Infrastructure in many enterprise companies is aging. Systems
have grown organically over the last few decades. They have integrated
modern applications for planning, BI, reporting, data storage, and
custom proprietary applications into much of the outdated
infrastructure. However the result has been that we continue to make
the infrastructure more complex and difficult to manage.  Complex
systems mix SFTP, web services, excel macros, multiple ERP systems all
together into data infrastucture that impacts stakeholders across the
entire company. When executive decisions come in to integrate new
applications, or upgrade existing applications features, it can
quickly become very risky and expensive to integrate the new
application with all of the existing infrastructure.

Techniques for managing the production and consumption of data across
many complex systems have become very standard and robust across the
high-tech, internet, and social media giants in silicon valley and
beyond, but enterprise manufacturing and business has been slow to
adopt these techniques. This is because the current data needs of most
manufacturing and business has been relatively small by comparison.
Data needs are rapidly growing, however, and this trend shows no sign
of slowing down. The need has arisen for scalable enterprise level
data infrastructure for multiple application integreation.

# VISION

As previously mentioned, the current data requirements of many large
enterprises are often quite small by the standards of the Big Data
world. Many business processes and planning workflows are working with
daily or weekly data updates to their systems, and it works well
enough for their current operational planning, execution, and S&OP
proceses. The idea with updating this infrastructure is to make
something that is mostly future-proof, with a robust data
infrastructure that can handle connecting to many sources of data, and
feeding that data to many different applications. This infrastructure
should be flexible enough to handle both real-time data streams and
batch processes to support the needs and requirements of different
business processes and applications. Other types of feeds that may be
integrated into such a system could include real-time IoT data coming
from asset tracking hardware, integrations with suppliers, vendors,
and customer data infrastructure, VMI-related data feeds, and maybe
even more unforeseen requirements. The idea is to create an
infrastructure so robust and scalable that integrating data from any
source into your enterprise infrastructure is as painless as possible.

# MINIMUM VIABLE PLATFORM


The full scope of the project is an ambitious and broad concept that
can include many challenges from different fields and technologies. In
the interest of producing a viable platform as quickly as possible, it
is important to consider which features are necessary to get a usable
product in place as quickly as possible. The features that have been
identified as fitting ito this set are as follows:

   * Stream data out of a a database designed to hold enterprise
business data from at least two different source schemas
   * Process data in stream into a scalable cloud Data Store
   * Reflect data changes in multiple source databases in an analytics
tool in real time

Due to lack of access to real instances of enterprise resource
planning software during the 2 week sprint on which the MVP of this
product will be developed, Postgres and MYSQL databases with slightly
different general schemas for the storage of typical enterprise data
(Inventories, orders, customers, suppliers, part master, BOM's) will
be devised and used for prototyping the stream data transformation
technology.

Instead of purchasing enterprise scale data visualization or planning
tools, some simple visualisations will be presented through a Flask
application to show the results of the platforms data streaming and
transformation processes.

# TECHNOLOGY SELECTION

