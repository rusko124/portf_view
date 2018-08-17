# coding: utf-8


class BaseCheck:
    score = 10
    message = None

    def __init__(self, data):
        self.data = data

    @property
    def is_mobile(self):
        return self.data['isMobile'] == '1'

    @property
    def mouse_x(self):
        return int(self.data['mX'])

    @property
    def mouse_y(self):
        return int(self.data['mY'])

    def is_error(self):
        raise NotImplementedError


class InvalidTouchDevice(BaseCheck):
    message = 'Touch device is not mobile'
    score = 5

    def is_error(self):
        return self.data['isTouch'] == '1' and not self.is_mobile


class IEWithoutActiveX(BaseCheck):
    message = 'Internet Explorer without ActiveX'

    def is_error(self):
        return self.data['jsV'] != '1.99' and self.data['verIE'] and self.data['activeX'] == '0'


class ActiveXWithoutIE(BaseCheck):
    message = 'ActiveX without Internet Explorer'

    def is_error(self):
        return self.data['jsV'] != '1.99' and not self.data['verIE'] and self.data['activeX'] != '0'


class NoCookies(BaseCheck):
    message = 'Cookies are not available'

    def is_error(self):
        return self.data['jsCook'] == '0' and self.data['rCook'] == '0'


class SmallDesktopScreen(BaseCheck):
    message = 'Too small screen for non-mobile devices'

    def is_error(self):
        return not self.is_mobile and (
            int(self.data['scrW']) < 200 or
            int(self.data['scrH']) < 200 or
            int(self.data['clW']) < 300 or
            int(self.data['clH']) < 300
        )


class SmallMobileScreen(BaseCheck):
    message = 'Too small screen for mobile devices'

    def is_error(self):
        return self.is_mobile and int(self.data['scrW']) < 50 or int(self.data['scrH']) < 50


class LargeMobileScreen(BaseCheck):
    message = 'Too large screen for mobile devices'

    def is_error(self):
        return int(self.data['scrW']) > 4000 or int(self.data['scrH']) > 4000


class NoScreenDimensions(BaseCheck):
    message = 'There are no dimensions for the screen'

    def is_error(self):
        return not self.data['scrW'] or not self.data['scrH']


class InvalidColorDepth(BaseCheck):
    message = 'Invalid color depth'

    def is_error(self):
        color_depth = int(self.data['cBit'])
        return color_depth < 8 or color_depth > 32


class NoCapabilities(BaseCheck):
    message = 'There is no Flash, HTML5 & Java'

    def is_error(self):
        return self.data['flash'] == '0' and self.data['html5v'] == '0' and self.data['java'] == '0'


class NoFlashVersion(BaseCheck):
    message = 'There is a Flash plugin but version is not available'

    def is_error(self):
        return 'Safari' not in self.data['jsua'] and self.data['flash'] == '1' and self.data['flashV'] == '0'


class TooLongOnSite(BaseCheck):

    @property
    def message(self):
        return 'More then 1 hour on site (%s seconds)' % int(self.data['tos'])

    def is_error(self):
        return int(self.data['tos']) > 3600


class ToManyPV(BaseCheck):

    @property
    def message(self):
        return 'To many page views (%s)' % int(self.data['pv'])

    def is_error(self):
        return int(self.data['pv']) > 20


class TooManyFocusChanges(BaseCheck):

    @property
    def message(self):
        return 'Too many focus changes (%s)' % int(self.data['fC'])

    def is_error(self):
        return int(self.data['fC']) > 200


class BadIE(BaseCheck):
    message = 'IE detected and js version is 1.99'

    def is_error(self):
        return self.data['verIE'] != '0' and self.data['jsV'] == '1.99'


class NoJSON(BaseCheck):
    message = 'There is no JSON object'

    def is_error(self):
        return self.data['json'] != '1'


class InvalidTimezone(BaseCheck):
    message = 'Invalid timezone'

    def is_error(self):
        return self.data['city'] and self.data['cTZ'] in ('undefined', 'null', 'NA') or self.data['cTZ'] != self.data[
            'cTZ2'] and self.data['ctype'] != 'Cellurar'


class TimeNotAvailable(BaseCheck):
    message = 'Current time is not available'

    def is_error(self):
        return not self.data['cTime'] or self.data['cTime'] == 'NA'


class TooManyActions(BaseCheck):
    message = 'Too many mouse moves or clicks'

    def is_error(self):
        return self.data['ic'] == '0' and \
               int(self.data['tosA']) <= 10 and \
               (int(self.data['mousemove']) > 1000 or int(self.data['click']) > 20)


class EarlyStart(BaseCheck):
    message = 'There are some actions on very beginning'

    def is_error(self):
        return self.data['ic'] == '0' and int(self.data['tosA']) >= 0 and int(self.data['pv']) == 0


class TooFastActions(BaseCheck):
    message = 'Too fast actions at the beginning'

    def is_error(self):
        return self.data['ic'] == '0' and int(self.data['tosA']) == 0 and (
            int(self.data['mousemove']) > 50 or int(self.data['scroll']) > 50 or int(self.data['keypress']) > 50 or int(
                self.data['click']) > 50)


class TooFastFocusChanges(BaseCheck):
    message = 'Too fast focus changes'

    def is_error(self):
        return int(self.data['fC']) > 10 and int(self.data['fC']) / 2 > int(self.data['pv'])


class NoNumericIP(BaseCheck):
    message = 'There is no numeric IP address'

    def is_error(self):
        return self.data['ipL'] == '0'


class IsIframe(BaseCheck):
    message = 'Iframe is bad'

    def is_error(self):
        return self.data['ic'] == '0' and self.data['cs'] == '1'


class HiddenClicks(BaseCheck):
    message = 'Clicks on hidden links'

    def is_error(self):
        return self.data['hc'] == '1'


class EmptyURL(BaseCheck):
    message = 'Current url is empty'

    def is_error(self):
        return not self.data['aURL']


class CursorOutOfTheScreen(BaseCheck):

    @property
    def message(self):
        return 'Cursor is out of the screen (%s; %s)' % (self.mouse_x, self.mouse_y)

    def is_error(self):
        return not self.is_mobile and (
            self.mouse_x > int(self.data['scrW']) or
            self.mouse_y > int(self.data['scrH'])
        ) and (
            self.mouse_x > int(self.data['clW']) or
            self.mouse_y > int(self.data['clH'])
        )


class InvalidCursorCoordinates(BaseCheck):
    message = 'Cursor coordinates is not a whole numbers'

    def is_error(self):
        return not self.is_mobile and self.data['verIE'] == '0' and (
            not is_integer(self.data['mX']) or not is_integer(self.data['mY']))


class InvalidURL(BaseCheck):
    message = 'Invalid URL value'

    def is_error(self):
        return self.data['ic'] == '1' and self.data['type'] == '3' and self.data['aURL'] in ('http:', 'https:')


class BadCursorPosition(BaseCheck):

    @property
    def message(self):
        return 'Cursor is on the top left corner of the screen (%s; %s)' % (self.mouse_x, self.mouse_y)

    def is_error(self):
        return not self.is_mobile and int(self.data['tosA']) > 5 and (
            int(self.data['mousemove']) > 0 or
            int(self.data['click']) > 0
        ) and (
            self.mouse_x == 0 and
            self.mouse_y == 0
        )


class Cloaking(BaseCheck):
    message = 'Cloaking is detected'

    def is_error(self):
        return self.data['ic'] == '1' and self.data['type'] != '3' and self.data['cloak'] == '1'


class TooManyChanges(BaseCheck):

    @property
    def message(self):
        return 'Too many changes (%s)' % self.data['chg']

    def is_error(self):
        return int(self.data['chg']) > 3


def is_integer(value):
    return str(int(float(value))) == value
