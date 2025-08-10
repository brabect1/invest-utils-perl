import unittest
import os
import sys

# add `xfrs` source tree into PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import xfrs

class TestSyntaxElements(unittest.TestCase):

    def setUp(self):
        pass


    def test_create(self):
        """Tests creating a normal instance using correct arguments."""
        p = xfrs.Price(1.2, 'USD')
        self.assertEqual( p.value, 1.2)
        self.assertEqual( p.currency, 'USD')
        self.assertEqual( p.fmt, None)

    def test_create_negative(self):
        """Tests creating an instance with negative value."""
        p = xfrs.Price(-1.2, 'EUR')
        self.assertEqual( p.value, -1.2)
        self.assertEqual( p.currency, 'EUR')
        self.assertEqual( p.fmt, None)

    def test_create_empty_currency(self):
        """Tests creating an instance with empty currency."""
        p = xfrs.Price(0.1, '')
        self.assertEqual( p.value, 0.1)
        self.assertEqual( p.currency, '')
        self.assertEqual( p.fmt, None)

    def test_create_fmt(self):
        """Tests creating an instance with a custom format."""
        p = xfrs.Price(1.2, 'USD', '{:f}')
        self.assertEqual( p.value, 1.2)
        self.assertEqual( p.currency, 'USD')
        self.assertEqual( p.fmt, '{:f}')

        # test that the class-default formatting unchanged
        self.assertEqual( xfrs.Price.fmt, '{:,.3f}')

    def test_create_value_None(self):
        """Tests raising `TypeError` when creating a new instance with a `None` value."""
        with self.assertRaises(TypeError):
            p = xfrs.Price(None, 'USD')

    def test_create_value_str(self):
        """Tests raising `TypeError` when creating a new instance with a string value."""
        with self.assertRaises(TypeError):
            p = xfrs.Price('1.2', 'USD')

    def test_create_value_int(self):
        """Tests creating a new instance with an integral value."""
        p = xfrs.Price(1, 'EUR')
        self.assertEqual( p.value, 1)
        self.assertEqual( p.currency, 'EUR')
        self.assertEqual( p.fmt, None)

    def test_create_currency_None(self):
        """Tests raising `TypeError` when creating a new instance with a `None` currency."""
        with self.assertRaises(TypeError):
            p = xfrs.Price('1.2', None)


if __name__ == '__main__':
    unittest.main()

