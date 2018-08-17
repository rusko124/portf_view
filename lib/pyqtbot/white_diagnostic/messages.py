# coding: utf-8
import re
from base64 import b64decode
from textwrap import dedent
from urllib.parse import parse_qsl, unquote_plus

from .checks import (
    ActiveXWithoutIE,
    BadCursorPosition,
    BadIE,
    Cloaking,
    CursorOutOfTheScreen,
    EarlyStart,
    EmptyURL,
    HiddenClicks,
    IEWithoutActiveX,
    InvalidColorDepth,
    InvalidCursorCoordinates,
    InvalidTimezone,
    InvalidTouchDevice,
    InvalidURL,
    IsIframe,
    LargeMobileScreen,
    NoCapabilities,
    NoCookies,
    NoFlashVersion,
    NoJSON,
    NoNumericIP,
    NoScreenDimensions,
    SmallDesktopScreen,
    SmallMobileScreen,
    TimeNotAvailable,
    ToManyPV,
    TooFastActions,
    TooFastFocusChanges,
    TooLongOnSite,
    TooManyActions,
    TooManyChanges,
    TooManyFocusChanges,
)


class WhiteDiagnosticMessage:
    """
    Wrapper around the data sent to White Diagnostic.
    """
    checks = (
        InvalidTouchDevice,
        IEWithoutActiveX,
        ActiveXWithoutIE,
        NoCookies,
        SmallDesktopScreen,
        SmallMobileScreen,
        LargeMobileScreen,
        NoScreenDimensions,
        InvalidColorDepth,
        NoCapabilities,
        NoFlashVersion,
        TooLongOnSite,
        ToManyPV,
        TooManyFocusChanges,
        BadIE,
        NoJSON,
        InvalidTimezone,
        TimeNotAvailable,
        TooManyActions,
        EarlyStart,
        TooFastActions,
        TooFastFocusChanges,
        NoNumericIP,
        IsIframe,
        HiddenClicks,
        EmptyURL,
        CursorOutOfTheScreen,
        InvalidCursorCoordinates,
        InvalidURL,
        BadCursorPosition,
        Cloaking,
        TooManyChanges,
    )
    _decoded = None

    def __init__(self, raw_data):
        self.raw_data = raw_data

    @property
    def decoded(self):
        if self._decoded is None:
            data = dict(parse_qsl(self.raw_data))
            matched_keys = sorted([k for k in data.keys() if re.match('p\d', k)])
            decoded = b64decode(''.join([data[key] for key in matched_keys]))
            data['payload'] = dict([item.split('=') for item in unquote_plus(decoded.decode()).split('|') if item])
            self._decoded = data
        return self._decoded

    @property
    def important_info(self):
        return dedent('''
        WD Message highlight:
          - Plugins: %(plg)s
          - Platform: %(platform)s
          - User agent: %(jsua)s
          - Flash version: %(flashV)s
          - Key presses: %(keypress)s
          - Language: %(lang)s
          - Timezone (client-side): %(cTZ)s
          - Timezone (server-side): %(cTZ2)s
          - Referrer: %(jsref)s
          - Screen dimensions: %(scrW)s x %(scrH)s''' % self.decoded['payload'])

    @property
    def score(self):
        decoded = self.decoded
        result = {}
        for check in self.checks:
            check = check(decoded['payload'])
            if check.is_error():
                result[check.message] = check.score
        return result

    @property
    def formatted(self):
        score = self.score
        if score:
            lines = '\n'.join([
                '%s: %s' % (key, value) for key, value in sorted(score.items())
            ])
            return '%s\nWhite Diagnostic score: %d' % (lines, sum(score.values()))
        return 'White Diagnostic score: 0'


# White Diagnostic payload fields description:
# aURL - page URL
# activeX - if ActiveX object is present on the page. Probably for IE detection.
# adbl - checks for `adblT` object, what is strange because there is ADBlock in my browser, but there is no such object
# brw - Browser name
# brwV - Browser version
# cBit - window.screen.colorDepth
# cTZ - timezone offset, detected on client-side
# cTZ2 - -//- but server-side
# cTime - current time on the client
# chg -
# cid - some ID
# cidG - first 10 symbols of `cid`
# cidS - another ID for session
# cidS3 - ?
# city - city detected on the server side
# clH - window height
# clW - window width
# click - counter for clicks, double clicks & touches
# cloak - results of cloaking check, which is based on page content
# counter - counter for sending data to WD.
# country - country, detected on the server side.
# cs - iframe check. 0 - not an iframe
# ctype - connection type, detected on the server side
# dev - device type
# ex - some strange CPU detection, based on intervals
# fC - counter for focus changes
# flash - if Flash plugin is available
# flashV - version of the Flash plugin
# frame - if the page is displayed in iframe
# hc - tracks if there was a click on hidden/not-visible link
# html5v - support for HTML5 video/audio
# ic - some internal value for campaign
# id3 - ?
# ip - client IP address
# ipL - numeric representation of the ip address
# isIm - some check for injected pixel - #_wd_img_pxl
# isMobile - if it is a mobile device
# isTouch - if it is a touch device
# isUA - if the User Agent value is different from the value in the cookie. 1 if it is a new value.
# isp - provider
# java - if JAVA plugin is enabled
# jsCook - check for some cookie value
# jsV - WD script version
# jshref - cleared version of window.location.href
# json - if JSON object is available
# jsref - cleared version of document.referrer
# jsua - cleared version of navigator.userAgent or navigator.vendor or window.opera
# keypress - counter for key presses
# lang - navigator.language
# lstor - if window.localStorage is available
# mX - X coordinate for current mouse position
# mY - Y coordinate for current mouse position
# mousemove - counter for `mousemove` events
# pid - some internal ID
# pjsref - some strange value from wd_ref cookie
# platform - navigator.platform
# plg - list of plugin's filenames separated by #
# pv - page views
# rCook - navigator.cookieEnabled
# refL - if the value from pjsref == `wd_ref` cookie value. Need more investigation
# sLang - navigator.systemLanguage
# scrH - screen.height
# scrR - window.devicePixelRatio or similar manual calculation result
# scrW - screen.width
# scroll - wheel events counter
# send - ? probably flag if the data was sent before
# sh - if human is detected. based on some scoring intervals
# sid - ID for account
# sstor - if window.sessionStorage is available
# tid - some trackID value from cookie
# tk - timestamp. probably for script download moment. last 4 digits - microseconds.
# tkn - backend token
# tos - seconds on site
# tosA - seems like seconds with actions
# tosV - seems like seconds when something was visible
# tsid - some subTrackID value from cookie
# type - some value from the backend
# uLang - navigator.userLanguage
# verIE - Internet Explorer version
# wRTC - if there is RTC capabilities available
