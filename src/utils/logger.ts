import { inspect } from 'util';

export enum LogLevel {
  TRACE = 0,
  DEBUG = 1,
  INFO = 2,
  WARN = 3,
  ERROR = 4,
}

export class Logger {
  private static _level: LogLevel = LogLevel.INFO;

  static get level(): LogLevel {
    return Logger._level;
  }

  static set level(level: LogLevel) {
    Logger._level = level;
  }

  static trace(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.TRACE) {
      console.trace(message, ...args.map(arg => Logger.formatArg(arg)));
    }
  }

  static debug(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.DEBUG) {
      console.debug(message, ...args.map(arg => Logger.formatArg(arg)));
    }
  }

  static info(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.INFO) {
      console.info(message, ...args.map(arg => Logger.formatArg(arg)));
    }
  }

  static warn(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.WARN) {
      console.warn(message, ...args.map(arg => Logger.formatArg(arg)));
    }
  }

  static error(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.ERROR) {
      console.error(message, ...args.map(arg => Logger.formatArg(arg)));
    }
  }

  private static formatArg(arg: any): any {
    if (typeof arg === 'object' && arg !== null) {
      return inspect(arg, { depth: null, colors: true });
    }
    return arg;
  }
}

// Set default log level from environment variable if present
const envLogLevel = process.env.LOG_LEVEL?.toUpperCase();
if (envLogLevel && envLogLevel in LogLevel) {
  Logger.level = LogLevel[envLogLevel as keyof typeof LogLevel];
} 