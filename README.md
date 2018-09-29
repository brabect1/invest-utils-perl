# invest-utils-perl
Misc perl utils for analysis of stock portfolio.

The utils takes form of a perl module (.pm) and some perl scripts using that
module. The module and scripts have ad hoc design to yield the desire function.
More structured approach will be chosen later once getting a good understanding
of the needed functions.

The `ALPHAVANTAGE_API_KEY` environment variable shall be set to quote stock and
currency prices through AlphaVantage API. That one is used as a secondary source
where the primary is Yahoo Finance.
