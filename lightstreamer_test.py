#
# py-Lightstreamer
# Copyright (C) 2012 David Wilson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import unittest

from lightstreamer import _decode_field


class DecodeFieldTestCase(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(_decode_field('$'), '')
        self.assertEqual(_decode_field('$', '$'), '')

    def test_null(self):
        self.assertTrue(_decode_field('#') is None)
        self.assertTrue(_decode_field('#', '#') is None)

    def test_prev(self):
        self.assertTrue(_decode_field('', None) is None)
        self.assertTrue(_decode_field('', '') == '')
        self.assertTrue(_decode_field('', '#') == '#')

    def test_prefix_strip(self):
        self.assertEqual(_decode_field('##'), '#')
        self.assertEqual(_decode_field('#$'), '$')
        self.assertEqual(_decode_field('$#'), '#')
        self.assertEqual(_decode_field('$$'), '$')

    def test_unicode_escape(self):
        self.assertEqual(_decode_field(r'\u2603'), u'\N{SNOWMAN}')


if __name__ == '__main__':
    unittest.main()
