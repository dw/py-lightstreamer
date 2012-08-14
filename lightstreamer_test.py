
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
