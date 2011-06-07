# ggg

a shunt to pull data from gmond/gmetad and stuff it into graphite

written on account of the slowness of ganglia-graphite.rb and to try my hand at go. i know the
goroutine calls here are unnecessary for such a trivial task, but it's
worth noting that doing so doesn't slow things down.

