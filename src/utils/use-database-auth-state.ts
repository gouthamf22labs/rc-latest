import {
  AuthenticationCreds,
  AuthenticationState,
  BufferJSON,
  initAuthCreds,
  proto,
  SignalDataTypeMap,
} from '@whiskeysockets/baileys';
import { Repository } from '../repository/repository.service';

export type DatabaseAuthState = {
  state: AuthenticationState;
  saveCreds: () => Promise<void>;
};

export async function useDatabaseAuthState(
  repository: Repository,
  instanceId: number,
): Promise<DatabaseAuthState> {
  const writeData = async (data: any, key: string): Promise<void> => {
    const json = JSON.parse(JSON.stringify(data, BufferJSON.replacer)) as object;
    await repository.session.upsert({
      where: { instanceId_sessionKey: { instanceId, sessionKey: key } },
      create: { instanceId, sessionKey: key, data: json },
      update: { data: json },
    });
  };

  const readData = async (key: string): Promise<any> => {
    const row = await repository.session.findUnique({
      where: { instanceId_sessionKey: { instanceId, sessionKey: key } },
    });
    if (!row) return null;
    return JSON.parse(JSON.stringify(row.data), BufferJSON.reviver);
  };

  const removeData = async (key: string): Promise<void> => {
    await repository.session
      .delete({ where: { instanceId_sessionKey: { instanceId, sessionKey: key } } })
      .catch(() => {});
  };

  const creds: AuthenticationCreds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids: string[]) => {
          const data: { [id: string]: SignalDataTypeMap[typeof type] } = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            }),
          );
          return data;
        },
        set: async (data: any) => {
          const tasks: Promise<void>[] = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(value ? writeData(value, key) : removeData(key));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => writeData(creds, 'creds'),
  };
}
