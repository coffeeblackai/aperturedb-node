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

  private static log(level: string, message: string, args: any[]) {
    const formattedArgs = args.map(arg => Logger.formatArg(arg));
    if (args.length > 0) {
      process.stdout.write(`[${level}] ${message} ${formattedArgs.join(' ')}\n`);
    } else {
      process.stdout.write(`[${level}] ${message}\n`);
    }
  }

  static trace(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.TRACE) {
      Logger.log('TRACE', message, args);
    }
  }

  static debug(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.DEBUG) {
      Logger.log('DEBUG', message, args);
    }
  }

  static info(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.INFO) {
      Logger.log('INFO', message, args);
    }
  }

  static warn(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.WARN) {
      Logger.log('WARN', message, args);
    }
  }

  static error(message: string, ...args: any[]) {
    if (Logger._level <= LogLevel.ERROR) {
      Logger.log('ERROR', message, args);
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