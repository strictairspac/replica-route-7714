import {swapTrialsApi} from '../api/swap_trials.js';
export const swapTrialsStore={items:[],async refresh(){this.items=await swapTrialsApi.list();return this.items;}};
