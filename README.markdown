Overview
--------

riakbloomutil simplifies the process of creating and updating bloom filters for the **riakbloom** server, and can be used to maintain filters based on data external to Riak.

Installation
------------

The application can be downloaded and built as follows:

    $ git clone git@github.com:whitenode/riakbloomutil.git
    $ cd riakbloomutil
    $ make

This will generate an application called **riakbloomutil** in the build directory. Before running this, make sure that the ebloom shared library (*ebloom_nifs.so*) that is created under *./deps/ebloom/priv* is available to be loaded. This can on Linux be achieved by either moving the library to another suitable location or updating the *LD_LIBRARY_PATH* environment to allow the system to find this library.

Usage
-----

The application allows the specification and creation of new filters as well as adding additional keys to an existing filter. The usage signature is as follows:

    Usage: ./riakbloomutil [-?] [-h <host>] [-p <port>] [-b <bucket>] [-f <filter>] [-u] [-E <elements>] [-P <probability>] [-S <seed>] <file>
    
      -?, --help		Show the program options
      -h, --host		Riak server host name or IP address. [default: '127.0.0.1']
      -p, --port		Riak server port. [default: 8087]
      -b, --bucket		Name of the bucket used for storing riakbloom filters. [default: 'riakbloom']
      -f, --filter		Name of the filter to be created or updated. Mandatory.
      -u, --update		Flag indicating existing filter is to be updated. If not set, a new filter will be created.
      -E, --elements	Estimated number of elements the filter will hold. Mandatory when creating filter.
      -P, --probability	Requested false positive probability expressed as float in interval [0 < P < 1]. Mandatory when creating filter.
      -S, --seed		Integer seed value to be used for new filter. [default: 0]
      <file>		Name of file containing filter keys. Mandatory.

When updating an existing filter, the **update** flag must be set and a filter name be provided through the **filter** parameter. The application will fetch the existing filter from Riak and then add the keys provided in the key file before storing it on the server. If no filter with the specified name could be found, it will generate an error. If siblings are identified, it will reconcile the filters through an ebloom *union* operation, before adding the specified keys. An example is shown below.

    ./riakbloomutil --update --name testfilter1 keyfile.txt

When creating a filter, the filter parameters **elements** (expected number of keys the filter will need to be able to hold) and **probability** (the desired false positive probability) must be provided in order to allow the system to determine the required size and configuration of the filter. An optional integer **seed** may also be provided. Please see the *ebloom* documentation for additional details regarding these parameters.

A filter name also need to be provided together with a key file as in the example below.

    ./riakbloomutil -n testfilter1 -E 10000 -P 0.001 keyfile.txt

If a filter with the specified name already exists in the system, the newly created filter will replace this.

The key file that needs to be supplied in both modes of operation is simple, and contains a list of keys separated by a single newline without any padding. No headers or comments are allowed as any text on a line will be treated as a key.






