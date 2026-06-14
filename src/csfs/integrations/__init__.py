# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Adapters that plug CSFS into downstream modelling frameworks.

Each integration module is import-safe without its target framework
installed (its framework base classes degrade to ``object``), so plain
``import csfs`` never gains a hard dependency from this package.

Currently available:

* :mod:`csfs.integrations.symfluence` — a SYMFLUENCE streamflow
  observation handler, auto-discovered through the
  ``symfluence.plugins`` entry-point group.
"""
